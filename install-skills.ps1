# ============================================================
#  Vertex Swarm Challenge — Skill Kit Installer
#  Rulează din rădăcina proiectului tău Claude Code
#  Exemplu: .\install-skills.ps1
# ============================================================

$ErrorActionPreference = "Stop"
$skillsDir = ".claude\skills"
$tempDir   = ".claude\_tmp"

function Write-Step($msg) {
    Write-Host "`n>> $msg" -ForegroundColor Cyan
}
function Write-OK($msg) {
    Write-Host "   OK  $msg" -ForegroundColor Green
}
function Write-Skip($msg) {
    Write-Host "   --  $msg (deja instalat)" -ForegroundColor DarkGray
}

# ── 0. Verificare Git ────────────────────────────────────────
Write-Step "Verificare Git..."
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "EROARE: Git nu e instalat. Descarca de la https://git-scm.com" -ForegroundColor Red
    exit 1
}
Write-OK "Git gasit"

# ── 1. Creare directoare ─────────────────────────────────────
Write-Step "Creare structura .claude/skills/..."
New-Item -ItemType Directory -Force -Path $skillsDir | Out-Null
New-Item -ItemType Directory -Force -Path $tempDir   | Out-Null
Write-OK "Directoare create"

# ── 2. Clone repos sursa ─────────────────────────────────────
$repos = @(
    @{ name = "anthropics-skills";         url = "https://github.com/anthropics/skills.git" },
    @{ name = "travisvn-skills";           url = "https://github.com/travisvn/awesome-claude-skills.git" },
    @{ name = "obra-superpowers";          url = "https://github.com/obra/claude-code-agent-skills.git" }
)

foreach ($repo in $repos) {
    $dest = "$tempDir\$($repo.name)"
    if (Test-Path $dest) {
        Write-Skip $repo.name
    } else {
        Write-Step "Clonez $($repo.name)..."
        git clone --depth 1 --quiet $repo.url $dest
        Write-OK $repo.name
    }
}

# ── 3. Copiere skill-uri ─────────────────────────────────────
$skillMap = @(
    # [sursa_relativa_in_tmp, nume_final_in_skills, prioritate]
    # --- MUST HAVE ---
    @{ src = "anthropics-skills\docx";              dst = "docx";                   prio = "must" },
    @{ src = "anthropics-skills\pdf";               dst = "pdf";                    prio = "must" },
    @{ src = "anthropics-skills\pptx";              dst = "pptx";                   prio = "must" },
    @{ src = "anthropics-skills\xlsx";              dst = "xlsx";                   prio = "opt"  },
    @{ src = "anthropics-skills\frontend-design";   dst = "frontend-design";        prio = "opt"  },
    @{ src = "anthropics-skills\product-self-knowledge"; dst = "product-self-knowledge"; prio = "opt" },

    # --- TRAVISVN community skills ---
    @{ src = "travisvn-skills\debugging";           dst = "debugging";              prio = "must" },
    @{ src = "travisvn-skills\code-review";         dst = "code-review";            prio = "rec"  },
    @{ src = "travisvn-skills\git-workflow";        dst = "git-workflow";           prio = "rec"  },
    @{ src = "travisvn-skills\readme-generator";    dst = "readme-generator";       prio = "must" },
    @{ src = "travisvn-skills\tdd";                 dst = "test-driven-development"; prio = "must" },
    @{ src = "travisvn-skills\rust";                dst = "rust-expert";            prio = "must" },
    @{ src = "travisvn-skills\systems-programming"; dst = "systems-programming";    prio = "must" },
    @{ src = "travisvn-skills\embedded";            dst = "embedded-systems";       prio = "must" },
    @{ src = "travisvn-skills\ros2";                dst = "ros2-workflow";          prio = "must" },
    @{ src = "travisvn-skills\concurrency";         dst = "concurrency-patterns";   prio = "rec"  },
    @{ src = "travisvn-skills\security";            dst = "security-hardening";     prio = "rec"  },
    @{ src = "travisvn-skills\mcp-builder";         dst = "mcp-builder";            prio = "rec"  },
    @{ src = "travisvn-skills\webapp-testing";      dst = "webapp-testing";         prio = "opt"  },

    # --- OBRA superpowers ---
    @{ src = "obra-superpowers\debugging";          dst = "obra-debugging";         prio = "rec"  },
    @{ src = "obra-superpowers\code-review";        dst = "obra-code-review";       prio = "rec"  }
)

Write-Step "Copiez skill-urile..."
$installed = 0
$skipped   = 0
$missing   = 0

foreach ($s in $skillMap) {
    $srcPath = "$tempDir\$($s.src)"
    $dstPath = "$skillsDir\$($s.dst)"

    if (Test-Path $dstPath) {
        Write-Skip $s.dst
        $skipped++
        continue
    }

    if (Test-Path $srcPath) {
        Copy-Item -Recurse -Force $srcPath $dstPath
        Write-OK "[$($s.prio.ToUpper())] $($s.dst)"
        $installed++
    } else {
        Write-Host "   ??  $($s.dst) — sursa nu gasita in repo (verifica manual)" -ForegroundColor Yellow
        $missing++
    }
}

# ── 4. Curatare temp ─────────────────────────────────────────
Write-Step "Curatare fisiere temporare..."
Remove-Item -Recurse -Force $tempDir
Write-OK "Temp sters"

# ── 5. Sumar ────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " GATA!  Skill Kit instalat in $skillsDir" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Instalate : $installed" -ForegroundColor Green
Write-Host "  Sarite    : $skipped"   -ForegroundColor DarkGray
Write-Host "  Lipsa     : $missing"   -ForegroundColor Yellow
Write-Host ""
Write-Host " Porneste Claude Code in proiect si scrie:" -ForegroundColor White
Write-Host "   claude"                                  -ForegroundColor Yellow
Write-Host " Claude va detecta skill-urile automat."   -ForegroundColor DarkGray
Write-Host ""
