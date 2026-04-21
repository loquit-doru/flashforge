"""
FlashForge Configuration Module
Pydantic settings, design presets, and constants for the FlashForge Agent Swarm
"""

from typing import Dict, List, Optional, Any
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum
from pathlib import Path


class LLMProvider(str, Enum):
    """Supported LLM providers"""
    GROQ = "groq"
    ANTHROPIC = "anthropic"
    QWEN = "qwen"
    GEMINI = "gemini"


class LogLevel(str, Enum):
    """Logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(BaseSettings):
    """Application settings with Pydantic validation"""

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent / ".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )
    
    # App Info
    APP_NAME: str = "FlashForge"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = Field(default=False, description="Debug mode")
    LOG_LEVEL: LogLevel = Field(default=LogLevel.INFO)
    
    # FoxMQ / Vertex Swarm Configuration
    SWARM_SECRET: str = Field(
        default="swarm-secret-change-in-prod",
        description="HMAC-SHA256 secret shared across all swarm nodes"
    )
    SWARM_DISCOVERY_PEERS: str = Field(
        default="",
        description="Comma-separated peer endpoints, e.g. localhost:5551,localhost:5552"
    )
    POC_LOG_DIR: str = Field(
        default="./poc_logs",
        description="Directory for Proof-of-Coordination JSONL logs"
    )
    COMMIT_WINDOW_MS: int = Field(
        default=500,
        description="ms to collect bids before committing a winner"
    )
    
    # LLM API Keys
    GROQ_API_KEY: str = Field(default="", description="Groq API key")
    ANTHROPIC_API_KEY: str = Field(default="", description="Anthropic API key")
    QWEN_API_KEY: str = Field(default="", description="Qwen (DashScope) API key")
    GEMINI_API_KEY: str = Field(default="", description="Google Gemini API key")
    
    # LLM Configuration
    PRIMARY_LLM: LLMProvider = Field(default=LLMProvider.GROQ)
    FALLBACK_LLM: LLMProvider = Field(default=LLMProvider.ANTHROPIC)  # Claude — best quality fallback (~$0.02/req)
    QUALITY_LLM: LLMProvider = Field(default=LLMProvider.ANTHROPIC)  # Claude Opus 4 — best quality
    
    # Model Selection
    GROQ_MODEL: str = Field(default="llama-3.3-70b-versatile")
    ANTHROPIC_MODEL: str = Field(default="claude-sonnet-4-6")
    QWEN_MODEL: str = Field(default="qwen3.5-flash")
    QWEN_BASE_URL: str = Field(default="https://dashscope-intl.aliyuncs.com/compatible-mode/v1")
    GEMINI_MODEL: str = Field(default="gemini-2.5-flash")
    GEMINI_BASE_URL: str = Field(default="https://generativelanguage.googleapis.com/v1beta/openai/")
    
    # Generation Parameters
    MAX_TOKENS: int = Field(default=32768)
    TEMPERATURE_PLANNER: float = Field(default=0.7)
    TEMPERATURE_BUILDER: float = Field(default=0.5)
    TEMPERATURE_CRITIC: float = Field(default=0.3)
    
    # Output Configuration
    OUTPUT_DIR: Path = Field(default=Path("./output"))
    TEMP_DIR: Path = Field(default=Path("./temp"))
    MAX_ZIP_SIZE_MB: int = Field(default=10)
    
    # Evaluation Weights
    WEIGHT_FUNCTIONALITY: float = Field(default=0.50)
    WEIGHT_DESIGN: float = Field(default=0.30)
    WEIGHT_SPEED: float = Field(default=0.20)
    
    # Quality Thresholds
    MIN_QUALITY_SCORE: float = Field(default=80.0)
    MAX_RETRY_ATTEMPTS: int = Field(default=5)
    
    @field_validator("OUTPUT_DIR", "TEMP_DIR", mode="before")
    @classmethod
    def validate_path(cls, v: Any) -> Path:
        """Ensure paths are Path objects"""
        if isinstance(v, str):
            return Path(v)
        return v


# Design Presets for Web Applications
DESIGN_PRESETS: Dict[str, Dict[str, Any]] = {
    "modern_minimal": {
        "name": "Modern Minimal",
        "description": "Clean, minimalist design with focus on content",
        "tailwind_config": {
            "colors": {
                "primary": "#0f172a",
                "secondary": "#64748b",
                "accent": "#3b82f6",
                "background": "#ffffff",
                "surface": "#f8fafc"
            },
            "fontFamily": {
                "sans": ["Inter", "system-ui", "sans-serif"]
            }
        },
        "cdn": [
            "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
        ]
    },
    "dark_cyberpunk": {
        "name": "Dark Cyberpunk",
        "description": "Neon-accented dark theme with futuristic vibes",
        "tailwind_config": {
            "colors": {
                "primary": "#00ff9f",
                "secondary": "#00b8ff",
                "accent": "#ff00ff",
                "background": "#0a0a0f",
                "surface": "#12121a"
            },
            "fontFamily": {
                "sans": ["Orbitron", "Rajdhani", "system-ui", "sans-serif"]
            }
        },
        "cdn": [
            "https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;600;700&family=Rajdhani:wght@300;400;500;600;700&display=swap"
        ]
    },
    "warm_organic": {
        "name": "Warm Organic",
        "description": "Warm, approachable design with earth tones",
        "tailwind_config": {
            "colors": {
                "primary": "#92400e",
                "secondary": "#d97706",
                "accent": "#f59e0b",
                "background": "#fffbeb",
                "surface": "#fef3c7"
            },
            "fontFamily": {
                "sans": ["Nunito", "system-ui", "sans-serif"]
            }
        },
        "cdn": [
            "https://fonts.googleapis.com/css2?family=Nunito:wght@300;400;500;600;700&display=swap"
        ]
    },
    "corporate_pro": {
        "name": "Corporate Pro",
        "description": "Professional, business-oriented design",
        "tailwind_config": {
            "colors": {
                "primary": "#1e40af",
                "secondary": "#3b82f6",
                "accent": "#60a5fa",
                "background": "#ffffff",
                "surface": "#eff6ff"
            },
            "fontFamily": {
                "sans": ["Roboto", "system-ui", "sans-serif"]
            }
        },
        "cdn": [
            "https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap"
        ]
    },
    "playful_colorful": {
        "name": "Playful Colorful",
        "description": "Vibrant, playful design for creative projects",
        "tailwind_config": {
            "colors": {
                "primary": "#7c3aed",
                "secondary": "#ec4899",
                "accent": "#fbbf24",
                "background": "#faf5ff",
                "surface": "#f3e8ff"
            },
            "fontFamily": {
                "sans": ["Poppins", "system-ui", "sans-serif"]
            }
        },
        "cdn": [
            "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap"
        ]
    }
}

# HTML Templates
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {{ tailwind_config | safe }}
    </script>
    {% for cdn in cdns %}
    <link rel="stylesheet" href="{{ cdn }}">
    {% endfor %}
    <style>
        {{ custom_css | safe }}
    </style>
</head>
<body class="bg-background text-primary min-h-screen">
    {{ content | safe }}
    <script>
        {{ custom_js | safe }}
    </script>
</body>
</html>"""

# Evaluation Criteria
EVALUATION_CRITERIA = {
    "functionality": {
        "weight": 0.50,
        "subcriteria": {
            "code_validity": 0.30,  # HTML/CSS/JS valid
            "interactivity": 0.25,   # Interactive elements work
            "responsiveness": 0.25,  # Mobile-friendly
            "completeness": 0.20     # All features implemented
        }
    },
    "design": {
        "weight": 0.30,
        "subcriteria": {
            "visual_appeal": 0.35,
            "consistency": 0.25,
            "typography": 0.20,
            "color_harmony": 0.20
        }
    },
    "speed": {
        "weight": 0.20,
        "subcriteria": {
            "generation_time": 0.50,
            "render_efficiency": 0.30,
            "code_optimization": 0.20
        }
    }
}

# Agent System Prompts
AGENT_PROMPTS = {
    "planner": """You are the Planner Agent for FlashForge. Analyze the user request and output a JSON plan for a single self-contained HTML file.
Output JSON with these keys: app_type, design_preset (dark_cyberpunk/modern_minimal/playful_colorful), layout (brief), components (list), features (list, min 3), tech_stack (css/javascript/icons), color_scheme (primary/secondary/accent/background), complexity (simple/medium/complex), quality_notes (list of 3 design tricks).
Be concise. Every feature the user mentions must appear in the plan.""",

    "builder": """You are the Builder Agent for FlashForge. Generate a single self-contained HTML file based on the plan.
Rules: Use Tailwind CDN, semantic HTML5, Google Fonts. All requested features must work. Use addEventListener (not inline onclick). Add hover states and transitions. Include real SVG icons with actual path data.
Output ONLY the complete HTML starting with <!DOCTYPE html>. No markdown fences, no explanations.""",

    "critic": """You are the Critic Agent for FlashForge. Evaluate HTML on: Functionality (50%), Design (30%), Speed (20%).
Be strict and honest. Output JSON with: scores (functionality/design/speed, each 0-100), suggestions (max 5 actionable items), feedback (1-2 sentences).""",

    "fixer": """You are the Fixer Agent for FlashForge. Improve the HTML based on critic scores. Fix the weakest dimension first. Keep all working features.
Output ONLY the complete fixed HTML starting with <!DOCTYPE html>. No markdown fences, no explanations."""
}

# Initialize settings
settings = Settings()


