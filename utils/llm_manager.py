"""
Multi-LLM Manager for BlitzDev
Handles Groq (primary/simple), Gemini (complex), Qwen (backup complex),
and Anthropic/Claude (fallback) with automatic fallback across ALL configured providers.

Resilience features:
  - Per-provider cooldown: skip provider for 15s after failure/429
  - Request timeout: 180s hard limit per API call
  - Last-successful-provider caching (warm path)
  - Content Validation Gate for short-response detection
"""

import asyncio
import time
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass
from enum import Enum
import json

import aiohttp
from groq import AsyncGroq
from openai import AsyncOpenAI  # used for Qwen (OpenAI-compatible API)
from anthropic import AsyncAnthropic

import sys
import os

# Ensure parent dir is importable (needed when running as script, not package)
_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent not in sys.path:
    sys.path.insert(0, _parent)

from config import settings, LLMProvider, LogLevel

# ── Resilience constants ─────────────────────────────────────────────
PROVIDER_COOLDOWN_SEC = 15        # skip provider for 15s after rate-limit (Groq recovers fast)
REQUEST_TIMEOUT_SEC = 300         # hard timeout per API call (Gemini 2.5 Flash needs ~60-120s with 65K tokens)


class LLMError(Exception):
    """Base exception for LLM errors"""
    pass


class LLMRateLimitError(LLMError):
    """Rate limit exceeded"""
    pass


class LLMTimeoutError(LLMError):
    """Request timeout"""
    pass


@dataclass
class LLMResponse:
    """Standardized LLM response"""
    content: str
    provider: LLMProvider
    model: str
    tokens_used: Optional[int] = None
    generation_time: float = 0.0
    success: bool = True
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class LLMManager:
    """Manages multiple LLM providers with fallback logic"""

    def __init__(self):
        self.clients: Dict[LLMProvider, Any] = {}
        self._init_clients()
        self.request_history: List[Dict[str, Any]] = []
        # Resilience state (borrowed from defi-agent)
        self._cooldowns: Dict[LLMProvider, float] = {}  # provider → cooldown-until timestamp
        self._last_successful: Dict[str, LLMProvider] = {}  # role → last provider that worked
        # Economy hook — called after every successful LLM call with provider name
        self._on_llm_spend: Optional[Callable[[str], None]] = None

    def set_spend_hook(self, hook: Callable[[str], None]) -> None:
        """Register a callback invoked after each successful LLM call.

        The hook receives the provider name (e.g. 'groq', 'gemini') so the
        caller can deduct credits from the Agent Economy.
        """
        self._on_llm_spend = hook
        
    def _init_clients(self):
        """Initialize LLM clients"""
        # Groq Client (primary/simple — FREE, ultra-fast)
        if settings.GROQ_API_KEY:
            self.clients[LLMProvider.GROQ] = AsyncGroq(
                api_key=settings.GROQ_API_KEY
            )
        
        # Qwen Client (complex — FREE, OpenAI-compatible via DashScope International)
        if settings.QWEN_API_KEY:
            self.clients[LLMProvider.QWEN] = AsyncOpenAI(
                api_key=settings.QWEN_API_KEY,
                base_url=settings.QWEN_BASE_URL
            )
        
        # Gemini Client (complex — FREE, OpenAI-compatible via Google AI)
        if settings.GEMINI_API_KEY:
            self.clients[LLMProvider.GEMINI] = AsyncOpenAI(
                api_key=settings.GEMINI_API_KEY,
                base_url=settings.GEMINI_BASE_URL
            )
        
        # Anthropic/Claude Client (fallback — $$ but best quality)
        if settings.ANTHROPIC_API_KEY:
            self.clients[LLMProvider.ANTHROPIC] = AsyncAnthropic(
                api_key=settings.ANTHROPIC_API_KEY
            )
    
    def _is_cooling_down(self, provider: LLMProvider) -> bool:
        """Check if provider is on cooldown (recently failed/429'd)."""
        until = self._cooldowns.get(provider)
        if not until:
            return False
        if time.time() > until:
            del self._cooldowns[provider]
            return False
        remaining = until - time.time()
        print(f"  ⏳ {provider.value} on cooldown ({remaining:.0f}s left) — skipping")
        return True

    def _set_cooldown(self, provider: LLMProvider, seconds: float = PROVIDER_COOLDOWN_SEC):
        """Put provider on cooldown after failure."""
        self._cooldowns[provider] = time.time() + seconds
        print(f"  🧊 {provider.value} → cooldown for {seconds:.0f}s")

    async def generate(
        self,
        prompt: str,
        provider: Optional[LLMProvider] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        system_prompt: Optional[str] = None,
        fallback: bool = True,
        min_content_length: int = 0
    ) -> LLMResponse:
        """
        Generate text using specified or primary provider with optional fallback.
        
        Content Validation Gate: if min_content_length > 0, a response shorter
        than that threshold is treated as a failure (triggers retry / fallback)
        even when the API call itself succeeded.  This catches Gemini's
        intermittent "degenerate short response" pattern.
        
        Args:
            prompt: User prompt
            provider: Specific provider to use (defaults to PRIMARY_LLM)
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            system_prompt: Optional system prompt
            fallback: Whether to try fallback providers on failure
            min_content_length: Minimum acceptable response length (0 = disabled)
        
        Returns:
            LLMResponse with generated content and metadata
        """
        provider = provider or settings.PRIMARY_LLM
        temperature = temperature if temperature is not None else settings.TEMPERATURE_BUILDER
        max_tokens = max_tokens or settings.MAX_TOKENS
        
        # ── Build smart fallback chain ───────────────────────
        # 3 providers: Groq (fast), Qwen (complex/free), Claude (quality/$)
        tagged_chain: List[tuple] = [(provider, "primary")]
        if fallback:
            smart_order: List[tuple] = []
            # Add all other providers as fallback
            for p in LLMProvider:
                if p != provider:
                    entry = (p, "primary")
                    smart_order.append(entry)
            for entry in smart_order:
                if entry not in tagged_chain:
                    tagged_chain.append(entry)
        
        # Warm path: if we know a provider that worked recently for this role,
        # move it to the front (after the requested provider).
        role_key = f"{provider.value}:{temperature}"
        warm = self._last_successful.get(role_key)
        if warm:
            warm_entry = (warm, "primary")
            if warm_entry in tagged_chain and warm_entry != tagged_chain[0]:
                tagged_chain.remove(warm_entry)
                tagged_chain.insert(1, warm_entry)
        
        last_error = None
        
        for prov, tag in tagged_chain:
            if prov not in self.clients:
                continue
            
            if self._is_cooling_down(prov):
                continue
            
            # Retry same provider once on short content
            max_attempts = 2 if (min_content_length and prov == provider and tag == "primary") else 1
            
            for attempt in range(1, max_attempts + 1):
                try:
                    start_time = time.time()
                    
                    # ── Wrap call with timeout ───────────────────────
                    coro = self._dispatch_provider(
                        prov, prompt, temperature, max_tokens, system_prompt
                    )
                    try:
                        response = await asyncio.wait_for(coro, timeout=REQUEST_TIMEOUT_SEC)
                    except asyncio.TimeoutError:
                        elapsed = time.time() - start_time
                        print(f"  ⏰ {prov.value} timed out after {elapsed:.0f}s")
                        self._set_cooldown(prov, 30)
                        last_error = LLMTimeoutError(f"{prov.value} timed out after {elapsed:.0f}s")
                        break  # move to next provider (already handled — no re-raise)
                    # ────────────────────────────────────────────────
                    
                    response.generation_time = time.time() - start_time
                    
                    # ── Content Validation Gate ──────────────────────
                    content_len = len(response.content or "")
                    if min_content_length and content_len < min_content_length:
                        print(
                            f"  ⚠ Content gate: {prov.value} returned {content_len} chars "
                            f"(need {min_content_length}) — attempt {attempt}/{max_attempts}"
                        )
                        if attempt < max_attempts:
                            await asyncio.sleep(1)   # brief backoff before retry
                            continue
                        # Exhausted retries — set brief cooldown.
                        last_error = ValueError(
                            f"{prov.value}: content too short ({content_len} < {min_content_length})"
                        )
                        break  # move to next provider
                    # ────────────────────────────────────────────────
                    
                    # Success! Cache warm path.
                    self._last_successful[role_key] = prov
                    self._log_request(prov, prompt, response)
                    # Notify economy hook (agent nodes deduct credits here)
                    if self._on_llm_spend:
                        try:
                            self._on_llm_spend(prov.value)
                        except Exception:
                            pass  # economy hook must never break LLM flow
                    return response
                    
                except Exception as e:
                    last_error = e
                    err_str = str(e).lower()
                    print(f"  ❌ {prov.value} error: {type(e).__name__}: {str(e)[:200]}")
                    if '429' in err_str or 'rate' in err_str:
                        self._set_cooldown(prov, 60)  # 60s — avoid hammering after quota exhaustion
                    elif '401' in err_str or 'auth' in err_str:
                        self._set_cooldown(prov, 300)  # 5min for auth errors
                    else:
                        self._set_cooldown(prov, 30)
                    break  # move to next provider
        
        # All providers failed
        return LLMResponse(
            content="",
            provider=LLMProvider.GROQ,
            model="",
            success=False,
            error=str(last_error) if last_error else "All providers failed",
            generation_time=0.0
        )
    
    async def _dispatch_provider(
        self,
        provider: LLMProvider,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: Optional[str],
    ) -> LLMResponse:
        """Dispatch to the correct provider generator."""
        if provider == LLMProvider.GROQ:
            return await self._generate_groq(prompt, temperature, max_tokens, system_prompt)
        elif provider == LLMProvider.ANTHROPIC:
            return await self._generate_anthropic(prompt, temperature, max_tokens, system_prompt)
        elif provider == LLMProvider.QWEN:
            return await self._generate_qwen(prompt, temperature, max_tokens, system_prompt)
        elif provider == LLMProvider.GEMINI:
            return await self._generate_gemini(prompt, temperature, max_tokens, system_prompt)
        else:
            raise LLMError(f"Unknown provider: {provider}")

    async def _generate_groq(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: Optional[str]
    ) -> LLMResponse:
        """Generate using Groq"""
        client = self.clients[LLMProvider.GROQ]
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        # Groq max is 32768 — cap to avoid 400 errors when called as fallback
        effective_max = min(max_tokens, 32768)
        
        response = await client.chat.completions.create(
            model=settings.GROQ_MODEL,
            messages=messages,
            temperature=temperature,
            max_tokens=effective_max
        )
        
        return LLMResponse(
            content=response.choices[0].message.content,
            provider=LLMProvider.GROQ,
            model=settings.GROQ_MODEL,
            tokens_used=response.usage.total_tokens if response.usage else None
        )
    
    async def _generate_anthropic(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: Optional[str]
    ) -> LLMResponse:
        """Generate using Anthropic (streaming to avoid 10-min timeout with Opus)"""
        client = self.clients[LLMProvider.ANTHROPIC]
        
        # Opus 4 requires streaming for long operations (SDK enforces this)
        chunks = []
        input_tokens = 0
        output_tokens = 0
        async with client.messages.stream(
            model=settings.ANTHROPIC_MODEL,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt or "",
            messages=[{"role": "user", "content": prompt}]
        ) as stream:
            async for text in stream.text_stream:
                chunks.append(text)
        
        response = await stream.get_final_message()
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        
        return LLMResponse(
            content="".join(chunks),
            provider=LLMProvider.ANTHROPIC,
            model=settings.ANTHROPIC_MODEL,
            tokens_used=input_tokens + output_tokens
        )
    
    async def _generate_qwen(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: Optional[str]
    ) -> LLMResponse:
        """Generate using Qwen via DashScope (OpenAI-compatible API)"""
        client = self.clients[LLMProvider.QWEN]
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        response = await client.chat.completions.create(
            model=settings.QWEN_MODEL,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens
        )
        
        return LLMResponse(
            content=response.choices[0].message.content,
            provider=LLMProvider.QWEN,
            model=settings.QWEN_MODEL,
            tokens_used=response.usage.total_tokens if response.usage else None
        )
    
    # Gemini 2.5 Flash is a thinking model: "thinking" tokens consume from the
    # max_tokens budget.  With max_tokens=20000 we measured 19197 thinking tokens
    # leaving only 799 for visible output (2370 chars → 2/7 sections).
    # With 65536 the model self-regulates thinking and produces full output.
    GEMINI_MAX_TOKENS = 65536

    async def _generate_gemini(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: Optional[str]
    ) -> LLMResponse:
        """Generate using Gemini via Google AI (OpenAI-compatible API)"""
        client = self.clients[LLMProvider.GEMINI]
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        # Inflate max_tokens for Gemini thinking models (2.5-*) — thinking tokens
        # consume from this budget.  Non-thinking models (2.0-*) don't need this.
        if "2.5" in settings.GEMINI_MODEL:
            effective_max = max(max_tokens, self.GEMINI_MAX_TOKENS)
        else:
            effective_max = min(max_tokens, 8192)  # non-thinking: keep it small & fast
        
        response = await client.chat.completions.create(
            model=settings.GEMINI_MODEL,
            messages=messages,
            temperature=temperature,
            max_tokens=effective_max
        )
        
        # ── Diagnostic logging ─────────────────────────────────
        content = response.choices[0].message.content or ""
        finish_reason = getattr(response.choices[0], "finish_reason", "unknown")
        usage = response.usage
        prompt_tokens = usage.prompt_tokens if usage else "?"
        completion_tokens = usage.completion_tokens if usage else "?"
        total_tokens = usage.total_tokens if usage else "?"
        
        # Calculate thinking tokens (total - prompt - completion)
        thinking_tokens = "?"
        if usage and usage.total_tokens and usage.prompt_tokens and usage.completion_tokens:
            thinking_tokens = usage.total_tokens - usage.prompt_tokens - usage.completion_tokens

        print(
            f"  📊 Gemini: finish={finish_reason} | "
            f"in={prompt_tokens} out={completion_tokens} think={thinking_tokens} | "
            f"chars={len(content)} | max_eff={effective_max}"
        )
        # ───────────────────────────────────────────────────────
        
        return LLMResponse(
            content=content,
            provider=LLMProvider.GEMINI,
            model=settings.GEMINI_MODEL,
            tokens_used=total_tokens if usage else None,
            metadata={"finish_reason": finish_reason}
        )
    
    async def generate_with_quality(
        self,
        prompt: str,
        temperature: float = 0.3,
        max_tokens: Optional[int] = None,
        min_content_length: int = 0
    ) -> LLMResponse:
        """Generate using quality-focused LLM with content validation gate."""
        return await self.generate(
            prompt=prompt,
            provider=settings.QUALITY_LLM,
            temperature=temperature,
            max_tokens=max_tokens,
            fallback=True,
            min_content_length=min_content_length
        )
    
    async def generate_parallel(
        self,
        prompt: str,
        providers: Optional[List[LLMProvider]] = None,
        temperature: Optional[float] = None
    ) -> List[LLMResponse]:
        """Generate with multiple providers in parallel"""
        providers = providers or [settings.PRIMARY_LLM, settings.FALLBACK_LLM]
        
        tasks = [
            self.generate(
                prompt=prompt,
                provider=prov,
                temperature=temperature,
                fallback=False
            )
            for prov in providers if prov in self.clients
        ]
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    def _log_request(
        self,
        provider: LLMProvider,
        prompt: str,
        response: LLMResponse
    ):
        """Log request for analytics"""
        self.request_history.append({
            "provider": provider.value,
            "prompt_length": len(prompt),
            "response_length": len(response.content),
            "tokens_used": response.tokens_used,
            "generation_time": response.generation_time,
            "success": response.success
        })
    
    def get_stats(self) -> Dict[str, Any]:
        """Get usage statistics"""
        if not self.request_history:
            return {"total_requests": 0}
        
        total = len(self.request_history)
        successful = sum(1 for r in self.request_history if r["success"])
        avg_time = sum(r["generation_time"] for r in self.request_history) / total
        total_tokens = sum(r["tokens_used"] or 0 for r in self.request_history)
        
        provider_stats = {}
        for r in self.request_history:
            prov = r["provider"]
            if prov not in provider_stats:
                provider_stats[prov] = {"count": 0, "success": 0}
            provider_stats[prov]["count"] += 1
            if r["success"]:
                provider_stats[prov]["success"] += 1
        
        return {
            "total_requests": total,
            "successful_requests": successful,
            "success_rate": successful / total,
            "average_generation_time": avg_time,
            "total_tokens": total_tokens,
            "provider_breakdown": provider_stats
        }
    
    async def health_check(self) -> Dict[str, bool]:
        """Check health of all configured providers.
        
        NOTE: Uses a snapshot of cooldowns and restores after, so health
        checks don't pollute the cooldown state for real builds.
        """
        saved_cooldowns = dict(self._cooldowns)
        results = {}
        
        for provider in LLMProvider:
            if provider not in self.clients:
                results[provider.value] = False
                continue
            
            try:
                response = await self.generate(
                    prompt="Say 'OK'",
                    provider=provider,
                    max_tokens=256,  # Gemini 2.5 Flash needs room for thinking tokens
                    fallback=False
                )
                results[provider.value] = response.success
            except Exception:
                results[provider.value] = False
        
        # Restore cooldowns — health check failures shouldn't block real builds
        self._cooldowns = saved_cooldowns
        return results


# Singleton instance
_llm_manager: Optional[LLMManager] = None


def get_llm_manager() -> LLMManager:
    """Get or create LLM manager singleton"""
    global _llm_manager
    if _llm_manager is None:
        _llm_manager = LLMManager()
    return _llm_manager


async def generate_text(
    prompt: str,
    provider: Optional[LLMProvider] = None,
    temperature: Optional[float] = None,
    system_prompt: Optional[str] = None
) -> str:
    """Convenience function for text generation"""
    manager = get_llm_manager()
    response = await manager.generate(
        prompt=prompt,
        provider=provider,
        temperature=temperature,
        system_prompt=system_prompt
    )
    return response.content if response.success else ""
