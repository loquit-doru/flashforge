"""
Lightweight DuckDuckGo web search for BlitzDev agent.
No API key needed. Used to ground answers with real, current information.
"""

import asyncio
import aiohttp
import re
from typing import List, Optional
from dataclasses import dataclass, field
from urllib.parse import urlparse


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str
    quality_tier: int = 0  # 0=unknown, 1=low, 2=medium, 3=high (authoritative)
    full_text: str = ""    # Scraped page content (trimmed) — empty if scrape failed


# ── Domain quality tiers ──────────────────────────────────────
# Tier 3: Official docs, specs, academic, CVE databases, engineering blogs
_TIER3_DOMAINS = {
    "arxiv.org", "github.com", "github.blog", "docs.github.com",
    "nvd.nist.gov", "cve.mitre.org",
    "anthropic.com", "openai.com", "developers.openai.com",
    "modelcontextprotocol.io", "spec.modelcontextprotocol.io",
    "cloud.google.com", "ai.google.dev",
    "microsoft.com", "techcommunity.microsoft.com", "azure.microsoft.com",
    "learn.microsoft.com", "devblogs.microsoft.com",
    "code.visualstudio.com",
    "docs.langchain.com", "python.langchain.com",
    "linuxfoundation.org", "cncf.io",
    "ieee.org", "acm.org", "nist.gov",
    "paloaltonetworks.com",  # Unit 42
    "darkreading.com",
    "a16z.com",
    "blog.jetbrains.com", "jetbrains.com",
    "slack.com", "docs.slack.dev",
    "huggingface.co",
    # JavaScript runtime official docs
    "nodejs.org", "deno.com", "deno.land", "docs.deno.com", "bun.sh",
    # Framework/platform official docs
    "nextjs.org", "remix.run", "astro.build", "svelte.dev", "vuejs.org",
    "reactjs.org", "react.dev", "angular.io", "angular.dev",
    "typescriptlang.org", "tc39.es", "ecma-international.org",
    "rust-lang.org", "doc.rust-lang.org", "go.dev", "golang.org",
    "python.org", "docs.python.org",
    # Cloud provider official docs
    "kubernetes.io", "k8s.io",
    "docs.aws.amazon.com", "aws.amazon.com",
    "docs.docker.com", "docker.com",
    "terraform.io", "hashicorp.com",
    "prometheus.io", "grafana.com",
    "owasp.org",
    # Edge/serverless platforms
    "workers.cloudflare.com", "developers.cloudflare.com",
    "vercel.com", "docs.netlify.com", "netlify.com",
    "supabase.com", "docs.supabase.com",
    # Engineering blogs from top companies
    "engineering.fb.com", "engineering.atspotify.com",
    "netflixtechblog.com", "blog.cloudflare.com",
    "eng.uber.com", "engineering.linkedin.com",
    "stripe.com", "docs.stripe.com",
    "security.googleblog.com",
    "ai.meta.com", "research.facebook.com",
    "deepmind.google",
}
# Tier 2: Reputable tech media, company blogs, well-known platforms
_TIER2_DOMAINS = {
    "tinybird.co", "vercel.com", "cloudflare.com", "supabase.com",
    "wired.com", "techcrunch.com", "theverge.com", "arstechnica.com",
    "infoworld.com", "zdnet.com", "theregister.com",
    "stackoverflow.com", "dev.to", "hackernews.com", "news.ycombinator.com",
    "replit.com", "sourcegraph.com",
    "docs.anthropic.com",
    "blog.google", "research.google",
    "cirra.ai", "agenticlabs.io",
    # Well-known vendor blogs with real data
    "cast.ai", "kubecost.com", "datadoghq.com",
    "sysdig.com", "aquasecurity.github.io", "snyk.io",
    "elastic.co", "redhat.com", "ibm.com",
    "cockroachlabs.com", "timescale.com", "planetscale.com",
    "figma.com", "notion.so",
    "thestack.technology", "developertech.com",
}
# Tier 1: Low quality — SEO farms, random blogs, content mills
_TIER1_PATTERNS = ["medium.com", "modelslab.com", "geeksforgeeks.org",
                    "analyticsvidhya.com", "towardsdatascience.com",
                    "freecodecamp.org", "w3schools.com",
                    "simplilearn.com", "javatpoint.com",
                    "tutorialspoint.com", "guru99.com"]


def _classify_domain(url: str) -> int:
    """Classify a URL into quality tiers: 3=authoritative, 2=reputable, 1=low, 0=unknown."""
    try:
        domain = urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return 0
    
    # Check tier 3 (authoritative)
    for d in _TIER3_DOMAINS:
        if domain == d or domain.endswith("." + d):
            return 3
    
    # Check tier 2 (reputable)
    for d in _TIER2_DOMAINS:
        if domain == d or domain.endswith("." + d):
            return 2
    
    # Check tier 1 (low quality)
    for pat in _TIER1_PATTERNS:
        if pat in domain:
            return 1
    
    return 0  # unknown — treat as neutral


# DuckDuckGo HTML search (no API key, no rate limit issues)
_DDG_URL = "https://html.duckduckgo.com/html/"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}


async def web_search(query: str, max_results: int = 5, timeout: float = 8.0) -> List[SearchResult]:
    """Search DuckDuckGo and return top results.
    
    Fast, free, no API key. Returns title + snippet for each result.
    Used to inject real-time context into LLM prompts.
    
    Args:
        query: Search query string
        max_results: Max results to return (default 5)
        timeout: Request timeout in seconds
        
    Returns:
        List of SearchResult with title, url, snippet
    """
    results: List[SearchResult] = []
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                _DDG_URL,
                data={"q": query, "b": ""},
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    return results
                html = await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return results
    
    # Parse results directly — extract parallel arrays of titles, URLs, snippets
    # (Block-based parsing breaks when DDG changes nesting depth)
    titles = re.findall(r'<a[^>]*class="result__a"[^>]*>(.*?)</a>', html, re.DOTALL)
    urls = re.findall(r'<a[^>]*class="result__a"[^>]*href="([^"]*)"', html)
    snippets = re.findall(r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', html, re.DOTALL)

    count = min(len(titles), len(snippets), max_results)
    for i in range(count):
        title = _strip_html(titles[i])
        url = urls[i] if i < len(urls) else ""
        snippet = _strip_html(snippets[i])
        if title or snippet:
            tier = _classify_domain(url)
            results.append(SearchResult(title=title, url=url, snippet=snippet, quality_tier=tier))
    
    return results


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#x27;", "'").replace("&nbsp;", " ")
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def format_search_context(results: List[SearchResult]) -> str:
    """Format search results as context for LLM injection.
    
    Sorts by quality tier (authoritative first) and marks source reliability.
    """
    if not results:
        return ""
    
    # Sort: tier 3 first, then 2, then rest  
    sorted_results = sorted(results, key=lambda r: -r.quality_tier)
    
    # Deduplicate by URL
    seen_urls = set()
    unique = []
    for r in sorted_results:
        if r.url not in seen_urls:
            seen_urls.add(r.url)
            unique.append(r)
    
    tier_labels = {3: "⭐ AUTHORITATIVE", 2: "✓ REPUTABLE", 1: "○ BLOG", 0: "○ OTHER"}
    
    lines = ["WEB SEARCH RESULTS (sorted by source reliability — prioritize authoritative sources):"]
    for i, r in enumerate(unique, 1):
        label = tier_labels.get(r.quality_tier, "")
        lines.append(f"[{i}] [{label}] {r.title}")
        if r.snippet:
            lines.append(f"    {r.snippet}")
        if r.url:
            lines.append(f"    Source: {r.url}")
    lines.append("")
    return "\n".join(lines)


async def multi_query_search(
    prompt: str,
    max_total: int = 12,
    timeout: float = 8.0,
) -> List[SearchResult]:
    """Run multiple targeted searches to get diverse, high-quality sources.
    
    Generates 2-3 search queries from the prompt:
    1. The original prompt (truncated)
    2. A more specific query targeting official/authoritative sources
    3. Optional: a third query for recent data/news
    
    Deduplicates by URL and sorts by quality tier.
    """
    queries = _generate_search_queries(prompt)
    per_query = max(4, max_total // len(queries))
    
    # Run all searches in parallel
    tasks = [web_search(q, max_results=per_query, timeout=timeout) for q in queries]
    all_results_lists = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Merge + deduplicate
    seen_urls: set = set()
    merged: List[SearchResult] = []
    for result_list in all_results_lists:
        if isinstance(result_list, Exception):
            continue
        for r in result_list:
            if r.url and r.url not in seen_urls:
                seen_urls.add(r.url)
                merged.append(r)
    
    # Sort by quality tier (authoritative first)
    merged.sort(key=lambda r: -r.quality_tier)
    
    return merged[:max_total]


def _generate_search_queries(prompt: str) -> List[str]:
    """Generate 2-3 targeted search queries from a prompt.
    
    Strategy:
    1. Main topic query (first 150 chars, cleaned)
    2. Authoritative source query — use site: operators to target official docs
    3. Data/benchmark query — target real numbers, comparisons, papers
    """
    # Extract core topic (first meaningful chunk)
    core = prompt[:200].strip()
    # Remove instruction-like suffixes
    for sep in ["Include specific", "I need", "I want", "Please", "cite your",
                "Give me", "Provide", "Help me", "Can you", "What are the"]:
        idx = core.lower().find(sep.lower())
        if idx > 30:
            core = core[:idx].strip().rstrip(".,;:")
    core = core[:150]
    
    queries = [core]  # Query 1: main topic (broad)
    
    # Extract key technical terms for focused queries
    words = core.split()
    key_terms = " ".join(words[:8]) if len(words) > 3 else core
    
    # Query 2: target authoritative sources using site: operators
    # Pick relevant site: domains based on topic keywords
    p_lower = prompt.lower()
    
    site_targets = []
    
    # Security / CVE / vulnerability topics
    if any(w in p_lower for w in ["security", "cve", "vulnerability", "exploit", "attack",
                                    "owasp", "pentest", "malware", "threat"]):
        site_targets = ["site:nvd.nist.gov", "site:owasp.org", "site:cve.mitre.org",
                        "site:darkreading.com"]
    
    # AI Agent Frameworks (LangChain, LlamaIndex, CrewAI, AutoGen, etc.)
    # Must be BEFORE generic "AI / ML / LLM" to catch specific framework names
    elif any(w in p_lower for w in ["langchain", "langgraph", "llamaindex", "llama-index",
                                     "llama_index", "crewai", "crew ai", "autogen",
                                     "agent framework", "ai agent framework",
                                     "multi-agent", "multi agent", "agentic"]):
        agent_sites = []
        if any(w in p_lower for w in ["langchain", "langgraph"]):
            agent_sites.append("site:python.langchain.com")
            agent_sites.append("site:langchain-ai.github.io")
        if any(w in p_lower for w in ["llamaindex", "llama-index", "llama_index"]):
            agent_sites.append("site:docs.llamaindex.ai")
        if any(w in p_lower for w in ["crewai", "crew ai"]):
            agent_sites.append("site:docs.crewai.com")
        if "autogen" in p_lower:
            agent_sites.append("site:microsoft.github.io/autogen")
        # Always include GitHub for benchmarks/repos and PyPI for downloads
        agent_sites.append("site:github.com")
        # Fallback if no specific framework detected
        if len(agent_sites) <= 1:
            agent_sites = ["site:python.langchain.com", "site:docs.llamaindex.ai",
                           "site:docs.crewai.com", "site:microsoft.github.io/autogen",
                           "site:github.com"]
        site_targets = agent_sites[:5]

    # AI / ML / LLM topics (generic)
    elif any(w in p_lower for w in ["llm", "gpt", "claude", "gemini", "ai model",
                                     "machine learning", "deep learning", "transformer",
                                     "mcp", "model context protocol", "agent", "rag"]):
        site_targets = ["site:arxiv.org", "site:openai.com", "site:anthropic.com",
                        "site:huggingface.co", "site:ai.meta.com"]
    
    # Kubernetes / Cloud / DevOps
    elif any(w in p_lower for w in ["kubernetes", "k8s", "docker", "container",
                                     "cloud", "aws", "gcp", "azure", "terraform",
                                     "devops", "ci/cd", "helm"]):
        site_targets = ["site:kubernetes.io", "site:docs.aws.amazon.com",
                        "site:cloud.google.com", "site:cncf.io"]
    
    # JavaScript runtimes (Node.js, Deno, Bun)
    elif any(w in p_lower for w in ["node.js", "nodejs", "deno", "bun", "javascript runtime",
                                     "js runtime", "server-side javascript", "cold-start",
                                     "cold start", "serverless benchmark"]):
        site_targets = ["site:nodejs.org", "site:deno.com", "site:bun.sh",
                        "site:github.com", "site:docs.deno.com"]
    
    # Meta-frameworks / SSR frameworks (Next.js, Remix, Astro, Nuxt, SvelteKit)
    # Must be BEFORE generic "web frameworks" to catch specific framework names
    elif any(w in p_lower for w in ["next.js", "nextjs", "remix", "astro",
                                     "nuxt", "sveltekit", "meta-framework",
                                     "meta framework", "ssr strategy",
                                     "server-side rendering framework"]):
        # Build targeted site list based on which frameworks are mentioned
        meta_sites = []
        if any(w in p_lower for w in ["next.js", "nextjs", "next "]):
            meta_sites.append("site:nextjs.org")
        if "remix" in p_lower or "react router" in p_lower:
            meta_sites.append("site:remix.run")
            meta_sites.append("site:reactrouter.com")
        if "astro" in p_lower:
            meta_sites.append("site:docs.astro.build")
            meta_sites.append("site:astro.build")
        if "nuxt" in p_lower:
            meta_sites.append("site:nuxt.com")
        if "sveltekit" in p_lower or "svelte" in p_lower:
            meta_sites.append("site:svelte.dev")
        # Always include github for benchmark repos
        meta_sites.append("site:github.com")
        # Fallback if no specific framework detected
        if len(meta_sites) <= 1:
            meta_sites = ["site:nextjs.org", "site:remix.run", "site:docs.astro.build",
                          "site:github.com"]
        site_targets = meta_sites[:5]
    
    # Web frameworks / Frontend (generic)
    elif any(w in p_lower for w in ["react", "vue", "angular", "svelte",
                                     "frontend", "web framework"]):
        site_targets = ["site:react.dev", "site:vuejs.org", "site:angular.dev",
                        "site:github.com", "site:vercel.com"]
    
    # Programming / Development
    elif any(w in p_lower for w in ["code review", "testing", "framework", "library",
                                     "programming", "software", "api", "sdk",
                                     "typescript", "python", "rust", "golang"]):
        site_targets = ["site:github.com", "site:stackoverflow.com",
                        "site:blog.jetbrains.com", "site:docs.microsoft.com"]
    
    # Database / Data
    elif any(w in p_lower for w in ["database", "sql", "nosql", "postgresql",
                                     "mongodb", "redis", "data pipeline"]):
        site_targets = ["site:github.com", "site:docs.aws.amazon.com",
                        "site:stackoverflow.com"]
    
    # Fallback: use github + official docs
    else:
        site_targets = ["site:github.com", "site:arxiv.org",
                        "site:stackoverflow.com"]
    
    # Build authoritative query with OR'd site: operators
    sites_part = " OR ".join(site_targets[:3])
    auth_terms = " ".join(words[:6]) if len(words) > 3 else core[:80]
    queries.append(f"{auth_terms} ({sites_part})")
    
    # Query 3: target real data, benchmarks, comparisons
    data_keywords = []
    if any(w in p_lower for w in ["compare", "vs", "versus", "benchmark", "performance"]):
        data_keywords = ["benchmark results data comparison"]
    elif any(w in p_lower for w in ["cost", "price", "pricing", "budget", "spend"]):
        data_keywords = ["real cost data pricing analysis report"]
    elif any(w in p_lower for w in ["security", "vulnerability", "cve"]):
        data_keywords = ["CVE advisory disclosure 2025 2026"]
    else:
        data_keywords = ["benchmark comparison data 2025 2026"]
    
    short_core = " ".join(words[:5]) if len(words) > 3 else core[:60]
    queries.append(f"{short_core} {data_keywords[0]}")
    
    return queries[:3]


def needs_web_search(prompt: str) -> bool:
    """Heuristic: does this prompt benefit from web search?
    
    Returns True for current events, factual questions, prices, news, etc.
    Returns False for creative writing, opinions, generic tasks.
    """
    p = prompt.lower()
    
    # Strong signals: needs current/real-time info
    _SEARCH_TRIGGERS = [
        "latest", "recent", "current", "today", "yesterday", "this week",
        "this month", "this year", "2024", "2025", "2026",
        "news", "update", "developments", "happening",
        "price of", "cost of", "how much does", "market",
        "who won", "who is winning", "election", "war",
        "stock", "crypto", "bitcoin", "ethereum", "solana",
        "weather", "score", "result",
        "statistics", "stats", "data on", "data about",
        "compare", "vs", "versus",
        "best", "top rated", "review of",
        "what happened", "what's happening", "what is happening",
        # Specific entities that need grounding
        "seedstr", "seedstr.io",
    ]
    
    for trigger in _SEARCH_TRIGGERS:
        if trigger in p:
            return True
    
    # Questions starting with "who is", "what is" often need facts
    if re.match(r'^(who is|what is|when did|where is|how many|how much)', p):
        return True
    
    return False


# ── Deep scraping: fetch actual page content ─────────────────────
_SCRAPE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# Domains that block scraping or return garbage
_SCRAPE_BLOCKLIST = {"arxiv.org", "pdf", "nvd.nist.gov"}


def _should_scrape(url: str) -> bool:
    """Check if URL is scrapable (not a PDF, not blocked)."""
    if not url:
        return False
    if url.endswith(".pdf"):
        return False
    try:
        domain = urlparse(url).netloc.lower()
    except Exception:
        return False
    for blocked in _SCRAPE_BLOCKLIST:
        if blocked in domain:
            return False
    return True


def _extract_text_from_html(html: str, max_chars: int = 3000) -> str:
    """Extract readable text from HTML, removing nav/footer/script noise."""
    # Remove scripts, styles, nav, footer, header
    for tag in ["script", "style", "nav", "footer", "header", "aside", "noscript"]:
        html = re.sub(rf'<{tag}[^>]*>.*?</{tag}>', '', html, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove all HTML tags
    text = re.sub(r'<[^>]+>', ' ', html)
    # Decode entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#x27;", "'").replace("&nbsp;", " ")
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove cookie/consent banners (common noise)
    for noise in ["cookie", "consent", "subscribe to our newsletter", "accept all"]:
        idx = text.lower().find(noise)
        if idx != -1 and idx < 300:
            text = text[idx + 100:]
    
    return text[:max_chars]


async def _scrape_page(session: aiohttp.ClientSession, url: str, timeout: float = 5.0) -> str:
    """Fetch and extract text from a single page. Returns empty string on failure."""
    try:
        async with session.get(
            url,
            headers=_SCRAPE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
            ssl=False,
        ) as resp:
            if resp.status != 200:
                return ""
            ct = resp.headers.get("content-type", "")
            if "text/html" not in ct and "application/xhtml" not in ct:
                return ""
            html = await resp.text(errors="replace")
            return _extract_text_from_html(html)
    except Exception:
        return ""


async def deep_scrape_results(
    results: List[SearchResult],
    max_pages: int = 5,
    timeout: float = 5.0,
) -> List[SearchResult]:
    """Scrape full content from top search results (tier 3 first).
    
    This gives the LLM REAL DATA from the pages, not just DDG snippets.
    Only scrapes top `max_pages` results (prioritizing authoritative sources).
    Returns the same list with `full_text` populated where successful.
    """
    # Pick top pages to scrape: tier 3 first, then tier 2, up to max_pages
    scrapable = [(i, r) for i, r in enumerate(results) if _should_scrape(r.url)]
    scrapable.sort(key=lambda x: -x[1].quality_tier)
    targets = scrapable[:max_pages]
    
    if not targets:
        return results
    
    async with aiohttp.ClientSession() as session:
        tasks = [_scrape_page(session, r.url, timeout) for _, r in targets]
        texts = await asyncio.gather(*tasks, return_exceptions=True)
    
    for (idx, _), text in zip(targets, texts):
        if isinstance(text, str) and len(text) > 100:
            results[idx].full_text = text
    
    return results


def format_search_context_deep(results: List[SearchResult]) -> str:
    """Format search results with full scraped content for maximum LLM context.
    
    For results with full_text: include up to 800 chars of real page content.
    For results without: fall back to snippet.
    This gives the LLM dramatically more real data to cite.
    """
    if not results:
        return ""
    
    # Sort: tier 3 first, then 2, then rest  
    sorted_results = sorted(results, key=lambda r: -r.quality_tier)
    
    # Deduplicate by URL
    seen_urls = set()
    unique = []
    for r in sorted_results:
        if r.url not in seen_urls:
            seen_urls.add(r.url)
            unique.append(r)
    
    tier_labels = {3: "⭐ AUTHORITATIVE", 2: "✓ REPUTABLE", 1: "○ BLOG", 0: "○ OTHER"}
    
    lines = ["WEB SEARCH RESULTS (sorted by source reliability — prioritize authoritative sources):"]
    for i, r in enumerate(unique, 1):
        label = tier_labels.get(r.quality_tier, "")
        lines.append(f"\n[{i}] [{label}] {r.title}")
        lines.append(f"    URL: {r.url}")
        
        if r.full_text and len(r.full_text) > 100:
            # Deep scraped content — real data from the page
            content = r.full_text[:800].strip()
            lines.append(f"    CONTENT: {content}")
        elif r.snippet:
            lines.append(f"    SNIPPET: {r.snippet}")
    
    lines.append("")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# CVE Validation — verify CVE IDs against NVD and fix descriptions
# ═══════════════════════════════════════════════════════════════

async def validate_cves_in_text(text: str, timeout: float = 3.0) -> str:
    """Find CVE-YYYY-NNNNN patterns in text and validate against NVD API.
    
    For each CVE mentioned:
    - Fetch real description from NVD (https://services.nvd.nist.gov/rest/json/cves/2.0)
    - If the CVE exists but the description in text is wrong → replace with real one
    - If the CVE doesn't exist → remove the hallucinated CVE or mark it
    
    Returns the corrected text.
    """
    import re as _cve_re
    
    # Find all CVE IDs in text
    cve_pattern = _cve_re.compile(r'CVE-\d{4}-\d{4,}')
    cve_ids = list(set(cve_pattern.findall(text)))
    
    if not cve_ids:
        return text
    
    # Fetch real CVE data from NVD API (parallel)
    cve_data = {}
    async with aiohttp.ClientSession() as session:
        tasks = []
        for cve_id in cve_ids[:10]:  # Max 10 CVEs to avoid slowdown
            tasks.append(_fetch_cve_from_nvd(session, cve_id, timeout))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for cve_id, result in zip(cve_ids[:10], results):
            if isinstance(result, dict):
                cve_data[cve_id] = result
    
    if not cve_data:
        return text  # NVD unreachable, return as-is
    
    # Now fix the text: for each CVE, check if description matches
    for cve_id, nvd_info in cve_data.items():
        if nvd_info.get("exists") is False:
            # CVE doesn't exist — mark it
            text = text.replace(cve_id, f"{cve_id} [unverified]")
            continue
        
        real_desc = nvd_info.get("description", "")
        cvss = nvd_info.get("cvss", "")
        
        if not real_desc:
            continue
        
        # Find the paragraph/section mentioning this CVE and replace description
        # Look for patterns like "CVE-2023-5528: <description>" or "CVE-2023-5528 — <description>"
        patterns = [
            # "CVE-XXXX-YYYY: old description here. More text."
            _cve_re.compile(
                _cve_re.escape(cve_id) + r'[:\s—–-]+([^\n.]{20,200}\.?)',
                _cve_re.IGNORECASE
            ),
        ]
        
        for pat in patterns:
            match = pat.search(text)
            if match:
                old_desc = match.group(1).strip()
                # Build replacement with real info
                short_desc = real_desc[:150]
                if len(real_desc) > 150:
                    short_desc = real_desc[:147] + "..."
                cvss_tag = f" (CVSS: {cvss})" if cvss else ""
                new_fragment = f"{cve_id}{cvss_tag}: {short_desc}"
                text = text[:match.start()] + new_fragment + text[match.end():]
                break
    
    return text


async def _fetch_cve_from_nvd(
    session: aiohttp.ClientSession, cve_id: str, timeout: float = 3.0
) -> dict:
    """Fetch CVE details from NVD API v2.0.
    
    Returns: {"exists": bool, "description": str, "cvss": str, "severity": str}
    """
    url = f"https://services.nvd.nist.gov/rest/json/cves/2.0?cveId={cve_id}"
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"Accept": "application/json"},
        ) as resp:
            if resp.status == 404:
                return {"exists": False}
            if resp.status != 200:
                return {}
            
            data = await resp.json()
            vulns = data.get("vulnerabilities", [])
            if not vulns:
                return {"exists": False}
            
            cve_item = vulns[0].get("cve", {})
            
            # Get English description
            descriptions = cve_item.get("descriptions", [])
            en_desc = ""
            for d in descriptions:
                if d.get("lang") == "en":
                    en_desc = d.get("value", "")
                    break
            
            # Get CVSS score
            cvss_score = ""
            metrics = cve_item.get("metrics", {})
            for version in ["cvssMetricV31", "cvssMetricV30", "cvssMetricV2"]:
                metric_list = metrics.get(version, [])
                if metric_list:
                    cvss_data = metric_list[0].get("cvssData", {})
                    score = cvss_data.get("baseScore", "")
                    severity = cvss_data.get("baseSeverity", "")
                    if score:
                        cvss_score = f"{score} {severity}".strip()
                    break
            
            return {
                "exists": True,
                "description": en_desc,
                "cvss": cvss_score,
            }
    except Exception:
        return {}

