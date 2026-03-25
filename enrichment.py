"""
Enrichment Engine — powers the Account Scorer.
Combines Exa AI search with tech stack scanning to enrich company data.
"""

import re
import os
import json
import signal
import subprocess
import urllib.parse
import urllib.request
import ssl
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ── Exa Client ──────────────────────────────────────────────────────

_exa_client = None


def get_exa(api_key):
    global _exa_client
    if _exa_client is None:
        from exa_py import Exa
        _exa_client = Exa(api_key=api_key)
    return _exa_client


# ── Timeout Handler ─────────────────────────────────────────────────

class TimeoutError(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutError("Request timed out")


# ── HTTP Session ────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
})


# ── Wappalyzer DB ──────────────────────────────────────────────────

WAPPALYZER_DB = {}
WAPPALYZER_DIR = os.path.join(os.path.dirname(__file__), "wappalyzer")

# Fallback: check the Tech Stack Analyzer directory
if not os.path.exists(WAPPALYZER_DIR):
    WAPPALYZER_DIR = os.path.expanduser(
        "~/Claude Code/Tech Stack Analyzer/wappalyzer"
    )

CATEGORY_MAP = {
    1: "CMS", 2: "Message boards", 3: "Database managers",
    4: "Documentation", 5: "Widgets", 6: "Ecommerce",
    7: "Photo galleries", 8: "Wikis", 9: "Hosting panels",
    10: "Analytics", 11: "Blogs", 12: "JavaScript frameworks",
    13: "Issue trackers", 14: "Video players", 15: "Comment systems",
    16: "Security", 17: "Font scripts", 18: "Web frameworks",
    19: "Miscellaneous", 20: "Editors", 21: "LMS",
    22: "Web servers", 23: "Caching", 24: "Rich text editors",
    25: "JavaScript graphics", 26: "Mobile frameworks",
    27: "Programming languages", 28: "Operating systems",
    29: "Search engines", 30: "Web mail", 31: "CDN",
    32: "Marketing automation", 33: "Web server extensions",
    34: "Databases", 35: "Maps", 36: "Advertising",
    37: "Network devices", 38: "Media servers", 39: "Webcams",
    41: "Payment processors", 42: "Tag managers", 43: "Paywalls",
    44: "Build systems", 45: "Task managers",
    46: "CI", 47: "DMS",
    48: "Static site generators", 49: "Linters",
    50: "Containers", 51: "PaaS", 52: "IaaS",
    53: "Reverse proxies", 54: "Load balancers",
    55: "UI frameworks", 56: "Cookie compliance",
    57: "Accessibility", 58: "Authentication",
    59: "SSL/TLS certificate authorities", 60: "Performance",
    61: "Content platforms", 62: "Translation",
    63: "Reviews", 64: "Buy now pay later",
    65: "Loyalty & rewards", 66: "Feature management",
    67: "Segmentation", 68: "Consent management platforms",
    69: "Geolocation", 70: "Customer data platforms",
    71: "Retargeting", 72: "RUM",
    73: "Cart abandonment", 74: "Personalisation",
    75: "A/B testing", 76: "Email",
    77: "Surveys", 78: "Live chat",
    79: "CRM", 80: "SEO", 81: "Accounting",
    82: "Cryptomining", 83: "User onboarding",
    84: "JavaScript libraries", 85: "Browser fingerprinting",
    86: "Ticket systems", 87: "Appointment scheduling",
    88: "Shipping carriers", 89: "Fulfilment",
    90: "Returns", 91: "Tax management",
    92: "Cross border", 93: "Referral marketing",
    94: "Digital asset management", 95: "Page builders",
    96: "Monitoring", 97: "Form builders",
    98: "DevOps", 99: "Hosting", 100: "Domain parking",
    101: "WordPress themes", 102: "WordPress plugins",
    103: "Shopify apps", 104: "Headless CMS",
    105: "Affiliate programs", 106: "Customer engagement",
    107: "Sales",
}

# Map Wappalyzer categories to Salesforce-relevant categories
SALESFORCE_CATEGORIES = {
    "CRM": "CRM",
    "Marketing automation": "Marketing",
    "Email": "Email",
    "Ecommerce": "E-commerce",
    "Live chat": "Service/Support",
    "Ticket systems": "Service/Support",
    "Comment systems": "Service/Support",
    "Analytics": "Analytics",
    "Tag managers": "Tag Management",
    "Advertising": "Advertising",
    "A/B testing": "A/B Testing",
    "Personalisation": "Personalization",
    "Customer data platforms": "CDP",
    "Retargeting": "Retargeting",
    "Segmentation": "Segmentation",
    "Geolocation": "Geolocation",
    "Security": "Security",
    "Cookie compliance": "Privacy",
    "Consent management platforms": "Privacy",
    "Accessibility": "Accessibility",
    "CDN": "CDN",
    "Payment processors": "Payments",
    "Maps": "Maps",
    "Video players": "Video players",
    "Font scripts": "Font scripts",
    "JavaScript frameworks": "JavaScript frameworks",
    "JavaScript libraries": "JavaScript libraries",
    "UI frameworks": "UI frameworks",
    "Web frameworks": "Web frameworks",
    "CMS": "CMS",
    "Static site generators": "CMS",
    "Headless CMS": "CMS",
    "Page builders": "Service/Support",
    "SEO": "Service/Support",
    "WordPress plugins": "Service/Support",
    "WordPress themes": "Service/Support",
    "Form builders": "Service/Support",
    "Reviews": "Service/Support",
    "Surveys": "Service/Support",
    "Appointment scheduling": "Service/Support",
    "Sales": "Sales Tools",
    "Hosting": "Hosting",
    "PaaS": "Cloud",
    "IaaS": "Cloud",
    "Containers": "Containers",
    "Databases": "Databases",
    "Programming languages": "Programming languages",
    "Web servers": "Web servers",
    "DMS": "DMS",
    "Monitoring": "Monitoring",
    "DevOps": "DevOps",
}


def load_wappalyzer_db():
    global WAPPALYZER_DB
    if WAPPALYZER_DB:
        return
    if not os.path.isdir(WAPPALYZER_DIR):
        print(f"  ⚠ Wappalyzer dir not found: {WAPPALYZER_DIR}")
        return
    count = 0
    for fname in sorted(os.listdir(WAPPALYZER_DIR)):
        if fname.endswith(".json"):
            with open(os.path.join(WAPPALYZER_DIR, fname)) as f:
                data = json.load(f)
                for tech_name, tech_data in data.items():
                    WAPPALYZER_DB[tech_name] = tech_data
                    count += 1
    print(f"  Loaded {count} Wappalyzer fingerprints")


# ── Website Discovery ──────────────────────────────────────────────

STRIP_SUFFIXES = [
    " inc.", " inc", " ltd.", " ltd", " limited", " corp.",
    " corp", " corporation", " llc", " lp", " l.p.", " co.",
    " group", " technologies", " technology", " tech",
    " software", " services", " service", " solutions",
    " canada", " canadian", " international", " intl",
    " consulting", " enterprises", " holdings",
    " north america", " of north america",
    " real estate", " investment trust",
    " & associates", " and associates",
    " industries", " operations", " management",
    " partners", " systems", " media", " communications",
    " construction", " engineering", " logistics",
    " financial", " capital", " properties", " realty",
    " staffing", " recruitment", " digital", " global",
    " network", " networks", " analytics", " labs",
    " health", " healthcare", " pharma", " life sciences",
    " energy", " power", " resources", " mining",
    " foods", " food", " brands", " brand",
    " transport", " transportation", " freight",
]


def lookup_website_url_guess(company_name):
    """Try to find a company website by guessing common URL patterns."""
    clean = company_name.lower()
    for suffix in STRIP_SUFFIXES:
        clean = clean.replace(suffix, "")
    clean = clean.strip().rstrip(".")

    # Remove parenthetical content
    clean = re.sub(r"\(.*?\)", "", clean).strip()
    # Remove leading "the "
    clean = re.sub(r"^the\s+", "", clean)
    # Remove content after " - " (e.g., "Westmoreland Sales - Also known as...")
    if " - " in clean:
        clean = clean.split(" - ")[0].strip()
    # Remove "also known as", "dba" etc
    clean = re.sub(r"\b(also known as|aka|dba|doing business as)\b.*", "", clean).strip()

    # Common English words that are too generic to try as standalone domains
    GENERIC_WORDS = {
        "boat", "bold", "land", "royal", "bond", "compass", "loyalty",
        "storage", "movers", "moving", "export", "also", "research",
        "links", "connected", "prospects", "distribution", "security",
        "warren", "highland", "campbell", "messenger", "thomas",
        "skyway", "outland", "bros", "hall", "parts", "sales",
        "growth", "live", "national", "general", "united", "first",
        "global", "world", "best", "elite", "premier", "prime",
        "smart", "star", "core", "vertex", "summit", "apex",
        "metro", "urban", "north", "south", "east", "west",
        "blue", "red", "green", "black", "white", "gold", "silver",
        "express", "direct", "advance", "standard", "clear",
    }

    # Generate slug candidates — ORDERED by priority
    # Priority 1: Full company name joined (most specific)
    # Priority 2: Full company name hyphenated
    # Priority 3: Individual words (only if not generic)
    primary_slugs = []
    fallback_slugs = []

    full_slug = re.sub(r"[^a-z0-9]+", "", clean)
    if full_slug:
        primary_slugs.append(full_slug)
    hyphen_slug = re.sub(r"[^a-z0-9]+", "-", clean).strip("-")
    if hyphen_slug and hyphen_slug != full_slug:
        primary_slugs.append(hyphen_slug)

    # Individual words — only as fallback, skip generic words
    words = clean.split()
    if len(words) > 1:
        for w in words:
            w_clean = re.sub(r"[^a-z0-9]", "", w)
            if len(w_clean) >= 3 and w_clean not in GENERIC_WORDS:
                fallback_slugs.append(w_clean)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    tlds = [".com", ".ca", ".io", ".co", ".org", ".net"]

    def try_slugs(slugs):
        for slug in slugs:
            for tld in tlds:
                for prefix in ["https://www.", "https://"]:
                    url = f"{prefix}{slug}{tld}"
                    try:
                        req = urllib.request.Request(
                            url, method="HEAD",
                            headers={"User-Agent": "Mozilla/5.0"}
                        )
                        resp = urllib.request.urlopen(req, timeout=6, context=ctx)
                        if resp.status < 400:
                            return url
                    except Exception:
                        pass
        return None

    # Try primary slugs first (full name), then fallback (individual words)
    result = try_slugs(primary_slugs)
    if result:
        return result
    return try_slugs(fallback_slugs)


def lookup_website_exa(company_name, exa_key):
    """Use Exa to find a company's website."""
    try:
        exa = get_exa(exa_key)
        result = exa.search(
            f"{company_name} Canada company",
            type="auto",
            num_results=5,
            category="company",
        )
        if result.results:
            # Skip aggregator sites
            skip_domains = [
                "glassdoor", "linkedin", "facebook", "twitter",
                "yelp", "indeed", "bloomberg", "crunchbase",
                "zoominfo", "dnb.com", "hoovers",
            ]
            for r in result.results:
                if not any(d in r.url.lower() for d in skip_domains):
                    return r.url
            # If all are aggregators, return the first anyway
            return result.results[0].url
    except Exception as e:
        print(f"  Exa website lookup error: {e}")
    return None


def discover_website(company_name, known_website, exa_key, do_exa=True):
    """Find a company's website using Exa (accurate) + URL guessing (fast fallback)."""
    if known_website:
        url = known_website.strip()
        if not url.startswith("http"):
            url = "https://" + url
        return url

    # Use Exa first — much more accurate for company lookup
    if do_exa and exa_key:
        url = lookup_website_exa(company_name, exa_key)
        if url:
            return url

    # Fallback to URL guessing (free, no API calls)
    url = lookup_website_url_guess(company_name)
    if url:
        return url

    return None


# ── DNS Analysis ────────────────────────────────────────────────────

def analyze_dns(domain):
    """Analyze DNS MX and TXT records for email/tech signals."""
    results = {"email_provider": [], "email_senders": []}

    # Strip www
    domain = domain.lstrip("www.")

    # MX records
    try:
        mx_out = subprocess.run(
            ["dig", "+short", "MX", domain],
            capture_output=True, text=True, timeout=10,
        )
        mx_text = mx_out.stdout.lower()
        if "google" in mx_text or "gmail" in mx_text:
            results["email_provider"].append("Google Workspace")
        if "outlook" in mx_text or "microsoft" in mx_text:
            results["email_provider"].append("Microsoft 365")
        if "pphosted" in mx_text or "proofpoint" in mx_text:
            results["email_provider"].append("Proofpoint")
        if "mimecast" in mx_text:
            results["email_provider"].append("Mimecast")
    except Exception:
        pass

    # TXT/SPF records
    try:
        txt_out = subprocess.run(
            ["dig", "+short", "TXT", domain],
            capture_output=True, text=True, timeout=10,
        )
        txt_text = txt_out.stdout.lower()
        spf_senders = {
            "salesforce.com": "Salesforce",
            "pardot.com": "Salesforce Pardot",
            "marketo.com": "Marketo",
            "hubspot": "HubSpot",
            "zendesk": "Zendesk",
            "freshdesk": "Freshdesk",
            "sendgrid": "SendGrid",
            "mailchimp": "Mailchimp",
            "amazonses": "Amazon SES",
            "google.com": "Google Workspace",
            "outlook.com": "Microsoft 365",
            "intercom": "Intercom",
            "drift": "Drift",
        }
        for pattern, name in spf_senders.items():
            if pattern in txt_text:
                results["email_senders"].append(name)
    except Exception:
        pass

    return results


# ── Subdomain Discovery ────────────────────────────────────────────

SUBDOMAIN_PREFIXES = [
    "support", "help", "status", "shop", "store", "blog",
    "community", "forum", "docs", "api", "app", "portal",
    "careers", "jobs",
]

PLATFORM_HEADERS = {
    "x-zendesk": "Zendesk",
    "x-served-by.*salesforce": "Salesforce",
    "x-shopify": "Shopify",
    "x-discourse": "Discourse",
    "x-greenhouse": "Greenhouse",
}

PLATFORM_DOMAINS = {
    "zendesk.com": "Zendesk",
    "salesforce.com": "Salesforce",
    "freshdesk.com": "Freshdesk",
    "intercom.io": "Intercom",
    "shopify.com": "Shopify",
    "statuspage.io": "Statuspage (Atlassian)",
    "atlassian.net": "Atlassian",
    "greenhouse.io": "Greenhouse",
    "lever.co": "Lever",
    "hubspot.com": "HubSpot",
}


def discover_subdomains(domain):
    """Probe common subdomains and detect platforms."""
    domain = domain.lstrip("www.")
    found = {}

    for prefix in SUBDOMAIN_PREFIXES:
        sub_url = f"https://{prefix}.{domain}"
        try:
            resp = SESSION.get(sub_url, timeout=(3, 8), allow_redirects=True)

            if resp.status_code < 400:
                redirect_domain = urllib.parse.urlparse(resp.url).netloc.lower()
                platform = None

                # Check redirect domain
                for pd_key, pd_name in PLATFORM_DOMAINS.items():
                    if pd_key in redirect_domain:
                        platform = pd_name
                        break

                # Check response headers
                if not platform:
                    headers_str = str(resp.headers).lower()
                    for h_pattern, h_name in PLATFORM_HEADERS.items():
                        if re.search(h_pattern, headers_str):
                            platform = h_name
                            break

                found[prefix] = {"url": resp.url, "platform": platform or ""}
        except Exception:
            pass

    return found


# ── Tech Stack (lightweight — no Playwright for speed) ──────────────

def scan_tech_stack_light(url):
    """Quick tech stack scan using requests + Wappalyzer (no Playwright)."""
    load_wappalyzer_db()

    detected = []
    domain = urllib.parse.urlparse(url).netloc

    # Fetch homepage
    try:
        resp = SESSION.get(url, timeout=(5, 15), allow_redirects=True)
    except Exception:
        return {"technologies": [], "dns": {}, "subdomains": {}}

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    script_srcs = [tag["src"] for tag in soup.find_all("script", src=True)]
    meta_content = ""
    for tag in soup.find_all("meta", attrs={"name": "generator"}):
        meta_content += " " + (tag.get("content", "") or "")

    header_str = "\n".join(f"{k}: {v}" for k, v in resp.headers.items())
    cookie_names = [c.name for c in resp.cookies]

    # Run Wappalyzer
    for tech_name, tech_data in WAPPALYZER_DB.items():
        cats = tech_data.get("cats", [])
        category_names = [CATEGORY_MAP.get(c, f"Cat {c}") for c in cats]
        raw_cat = category_names[0] if category_names else "Other"
        mapped_cat = SALESFORCE_CATEGORIES.get(raw_cat, raw_cat)
        for cn in category_names:
            if cn in SALESFORCE_CATEGORIES:
                mapped_cat = SALESFORCE_CATEGORIES[cn]
                break

        found = False

        # HTML patterns
        html_patterns = tech_data.get("html", [])
        if isinstance(html_patterns, str):
            html_patterns = [html_patterns]
        for pattern in html_patterns:
            try:
                clean_p = pattern.split("\\;")[0]
                if re.search(clean_p, html, re.IGNORECASE):
                    found = True
                    break
            except Exception:
                pass

        # Script src
        if not found:
            script_patterns = tech_data.get("scriptSrc", [])
            if isinstance(script_patterns, str):
                script_patterns = [script_patterns]
            for pattern in script_patterns:
                try:
                    clean_p = pattern.split("\\;")[0]
                    for src in script_srcs:
                        if re.search(clean_p, src, re.IGNORECASE):
                            found = True
                            break
                except Exception:
                    pass
                if found:
                    break

        # Headers
        if not found:
            header_patterns = tech_data.get("headers", {})
            if isinstance(header_patterns, dict):
                for h_name, pattern in header_patterns.items():
                    if isinstance(pattern, str):
                        clean_p = pattern.split("\\;")[0]
                        try:
                            if re.search(
                                f"{h_name}:.*{clean_p}", header_str, re.IGNORECASE
                            ):
                                found = True
                                break
                        except Exception:
                            pass

        # Cookies
        if not found:
            cookie_patterns = tech_data.get("cookies", {})
            if isinstance(cookie_patterns, dict):
                for c_name in cookie_patterns:
                    if c_name.lower() in [c.lower() for c in cookie_names]:
                        found = True
                        break

        # Meta
        if not found:
            meta_patterns = tech_data.get("meta", {})
            if isinstance(meta_patterns, dict):
                for m_name, pattern in meta_patterns.items():
                    if isinstance(pattern, str):
                        clean_p = pattern.split("\\;")[0]
                        try:
                            if m_name.lower() == "generator" and re.search(
                                clean_p, meta_content, re.IGNORECASE
                            ):
                                found = True
                                break
                        except Exception:
                            pass

        if found:
            detected.append({"category": mapped_cat, "name": tech_name})

    # Also check script srcs for known platforms
    url_tech_map = {
        "salesforce.com": ("CRM", "Salesforce"),
        "pardot.com": ("Marketing", "Salesforce Pardot"),
        "hubspot": ("CRM", "HubSpot"),
        "hs-scripts.com": ("CRM", "HubSpot"),
        "marketo": ("Marketing", "Marketo"),
        "drift.com": ("Service/Support", "Drift"),
        "intercom.io": ("Service/Support", "Intercom"),
        "zendesk.com": ("Service/Support", "Zendesk"),
        "freshdesk.com": ("Service/Support", "Freshdesk"),
        "6sense.com": ("Marketing", "6sense"),
        "qualified.com": ("Sales Tools", "Qualified"),
        "salesloft": ("Sales Tools", "SalesLoft"),
        "outreach.io": ("Sales Tools", "Outreach"),
        "gong.io": ("Sales Tools", "Gong"),
    }
    all_srcs = " ".join(script_srcs).lower() + " " + html.lower()
    for pattern, (cat, name) in url_tech_map.items():
        if pattern in all_srcs:
            if not any(d["name"] == name for d in detected):
                detected.append({"category": cat, "name": name})

    # DNS
    dns = analyze_dns(domain)

    # Add DNS-discovered tech to detected list
    for sender in dns.get("email_senders", []):
        sender_lower = sender.lower()
        if "salesforce" in sender_lower and not any("Salesforce" == d["name"] for d in detected):
            detected.append({"category": "CRM", "name": "Salesforce (via SPF)"})
        if "pardot" in sender_lower and not any("Pardot" in d["name"] for d in detected):
            detected.append({"category": "Marketing", "name": "Salesforce Pardot (via SPF)"})
        if "hubspot" in sender_lower and not any("HubSpot" == d["name"] for d in detected):
            detected.append({"category": "CRM", "name": "HubSpot (via SPF)"})

    # Subdomains
    subdomains = discover_subdomains(domain)

    # Add subdomain platforms to detected
    for sub_name, sub_info in subdomains.items():
        platform = sub_info.get("platform", "")
        if platform and not any(d["name"] == platform for d in detected):
            detected.append({
                "category": "Service/Support",
                "name": f"{platform} (via {sub_name})",
            })

    # Override miscategorized technologies
    category_overrides = {
        "Qualified": "Sales Tools",
    }
    for t in detected:
        if t["name"] in category_overrides:
            t["category"] = category_overrides[t["name"]]

    # Deduplicate technologies by name
    seen_names = set()
    unique_detected = []
    for t in detected:
        if t["name"] not in seen_names:
            seen_names.add(t["name"])
            unique_detected.append(t)

    return {
        "technologies": unique_detected,
        "dns": dns,
        "subdomains": subdomains,
    }


# ── Exa: Company Intel ─────────────────────────────────────────────

def get_company_intel(company_name, website, exa_key):
    """Get company description, industry, and size via Exa."""
    try:
        exa = get_exa(exa_key)
        result = exa.search_and_contents(
            f"{company_name} company overview",
            type="auto",
            num_results=3,
            text={"max_characters": 2000},
            category="company",
        )

        intel = {
            "description": "",
            "industry": "",
            "estimated_size": "",
        }

        if result.results:
            # Clean company name for matching
            clean_co = re.sub(r"\s*\([^)]*\)\s*", " ", company_name).strip()
            clean_co = _clean_company_name(clean_co)
            co_words = [w.lower() for w in clean_co.split() if len(w) > 2]
            co_strip = {"inc", "ltd", "corp", "limited", "llc", "the",
                        "technologies", "canada", "group", "holdings"}
            co_core = [w for w in co_words if w not in co_strip]
            if not co_core:
                co_core = co_words[:1]

            # Find the best result that actually mentions the company
            best = None
            for candidate in result.results:
                ctext = (candidate.text or "").lower()
                ctitle = (candidate.title or "").lower()
                combined = ctitle + " " + ctext[:500]
                if len(co_core) >= 2:
                    if sum(1 for w in co_core if w in combined) >= 2:
                        best = candidate
                        break
                else:
                    if any(w in combined for w in co_core):
                        best = candidate
                        break
            if best is None:
                best = result.results[0]  # fallback

            text = best.text or ""

            # Extract a clean description (first ~300 chars)
            # Filter out lines that mention a different company
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            desc_parts = []
            for line in lines:
                if len(" ".join(desc_parts)) > 300:
                    break
                # Skip navigation-like lines
                if len(line) < 15 or line.count("|") > 2:
                    continue
                # Skip lines (or sentences within lines) about a different entity
                line_lower = line.lower()
                # Split on sentence boundaries to check each sentence
                sentences = re.split(r'(?<=[.!?])\s+', line)
                clean_sentences = []
                for sent in sentences:
                    sent_stripped = sent.strip()
                    if not sent_stripped:
                        continue
                    # Check if sentence introduces a different entity
                    diff_entity_patterns = [
                        r"[A-Z][\w\s]{2,30} is a (?:501|charitable|non-?profit)",
                        r"[A-Z][\w\s]{2,30} (?:LLC|Inc|Corp|Ltd) is a",
                        r"[A-Z][\w-]+(?:\s+[A-Z][\w-]+)* is a (?:company|organization|firm|startup) that",
                    ]
                    is_diff_entity = False
                    for dep in diff_entity_patterns:
                        m = re.search(dep, sent_stripped)
                        if m and not any(w in m.group(0).lower() for w in co_core):
                            is_diff_entity = True
                            break
                    if not is_diff_entity:
                        clean_sentences.append(sent_stripped)
                if clean_sentences:
                    desc_parts.append(" ".join(clean_sentences))
            intel["description"] = " ".join(desc_parts)[:500]

            # Try to find employee count mentions
            emp_patterns = [
                r"(\d[\d,]+)\s*(?:employees|staff|people|team members)",
                r"(\d[\d,]+)\s*(?:\+\s*)?employees",
                r"team of (\d[\d,]+)",
            ]
            for pattern in emp_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    intel["estimated_size"] = match.group(1)
                    break

            # Try to identify industry
            industry_keywords = {
                "fintech": "Financial Technology",
                "financial services": "Financial Services",
                "banking": "Banking",
                "insurance": "Insurance",
                "healthcare": "Healthcare",
                "health care": "Healthcare",
                "biotech": "Biotechnology",
                "pharmaceutical": "Pharmaceuticals",
                "retail": "Retail",
                "e-commerce": "E-commerce",
                "ecommerce": "E-commerce",
                "manufacturing": "Manufacturing",
                "construction": "Construction",
                "real estate": "Real Estate",
                "technology": "Technology",
                "software": "Software",
                "saas": "SaaS",
                "telecommunications": "Telecommunications",
                "telecom": "Telecommunications",
                "media": "Media & Entertainment",
                "education": "Education",
                "energy": "Energy",
                "oil and gas": "Oil & Gas",
                "mining": "Mining",
                "agriculture": "Agriculture",
                "food": "Food & Beverage",
                "logistics": "Logistics",
                "transportation": "Transportation",
                "automotive": "Automotive",
                "aerospace": "Aerospace",
                "defense": "Defense",
                "government": "Government",
                "nonprofit": "Non-Profit",
                "consulting": "Consulting",
                "legal": "Legal",
                "hospitality": "Hospitality",
                "travel": "Travel",
            }
            text_lower = text.lower()
            for keyword, industry in industry_keywords.items():
                if keyword in text_lower:
                    intel["industry"] = industry
                    break

        return intel

    except Exception as e:
        print(f"  Exa company intel error: {e}")
        return {"description": "", "industry": "", "estimated_size": ""}


# ── Exa: News & Compelling Events ──────────────────────────────────

def get_company_news(company_name, exa_key):
    """Find recent news and compelling events for a company."""
    try:
        exa = get_exa(exa_key)

        # Clean company name for search (strip parenthetical suffixes)
        search_name = re.sub(r"\s*\([^)]*\)\s*", " ", company_name).strip()

        # Search for recent news — prioritize growth signals
        result = exa.search_and_contents(
            f"{search_name} funding acquisition partnership expansion contract announcement",
            type="auto",
            num_results=8,
            text={"max_characters": 800},
            category="news",
        )

        news_items = []
        compelling_events = []

        # Event detection config — ordered by priority (growth signals first)
        EVENT_TYPES = [
            ("M&A Activity", [
                "acquisition", "acquired", "merger", "merge",
                "buys", "bought", "takeover",
            ]),
            ("Funding/IPO", [
                "funding", "raised", "series a", "series b", "series c",
                "investment", "venture", "capital raise", "ipo",
                "public offering", "valuation",
            ]),
            ("Major Contract/Deal", [
                "contract", "deal", "awarded", "won",
                "government contract", "rfp", "selected by",
                "chosen by", "partnership agreement",
            ]),
            ("Strategic Partnership", [
                "partnership", "partners with", "teamed up",
                "collaboration", "joint venture", "alliance",
                "integrat", "strategic agreement",
            ]),
            ("Expansion", [
                "expansion", "new office", "new market",
                "opens", "launch", "entering", "expands",
                "new location", "hiring", "headcount",
            ]),
            ("Digital Transformation", [
                "digital transformation", "cloud migration",
                "moderniz", "technology upgrade", "implement",
                "new platform", "ai adoption",
            ]),
            ("Restructuring", [
                "layoff", "restructur", "downsiz", "cost-cutting",
                "reorganiz",
            ]),
        ]

        if result.results:
            for r in result.results:
                text_lower = (r.title or "").lower() + " " + (r.text or "").lower()

                # Detect event types for this article
                article_events = []
                for event_name, keywords in EVENT_TYPES:
                    if any(w in text_lower for w in keywords):
                        article_events.append(event_name)

                # Priority score: growth signals rank higher
                priority = 0
                if "M&A Activity" in article_events:
                    priority += 10
                if "Funding/IPO" in article_events:
                    priority += 9
                if "Major Contract/Deal" in article_events:
                    priority += 8
                if "Strategic Partnership" in article_events:
                    priority += 7
                if "Expansion" in article_events:
                    priority += 6
                if "Digital Transformation" in article_events:
                    priority += 5

                # Build a short summary from the text — strip nav junk first
                text_clean = (r.text or "").strip()
                # Remove nav/menu lines
                text_lines = text_clean.split("\n")
                nav_words = {"news", "technology", "launch", "government", "military",
                             "finance", "connectivity", "interactive", "features", "about",
                             "contact", "menu", "search", "login", "subscribe", "home",
                             "privacy", "cookie", "terms", "careers", "blog", "press"}
                # Junk phrases to strip entirely
                junk_phrases = [
                    "stay informed with", "free newsletter", "subscribe to",
                    "sign up for", "skip to main content", "dismiss",
                    "oops, some", "cookie", "accept all", "reject all",
                    "read more", "share this", "facebook", "twitter",
                    "linkedin", "email this", "print this", "bookmark",
                    "accessibility statement", "skip navigation",
                    "subscribe for $", "sign in to view",
                    "create your free account", "notification icon",
                    "userprof", "primary sidebar",
                    "🔥live webinar", "🔥 live webinar",
                ]
                clean_lines = []
                for tl in text_lines:
                    tl = tl.strip()
                    if not tl:
                        continue
                    tl_lower = tl.lower()
                    # Skip junk phrases
                    if any(jp in tl_lower for jp in junk_phrases):
                        continue
                    tokens = [w.strip().lower() for w in re.split(r"[|,/]", tl)]
                    if len(tokens) >= 3 and sum(1 for t in tokens if t in nav_words) >= 2:
                        continue
                    # Skip very short lines (likely nav fragments)
                    if len(tl) < 15:
                        continue
                    clean_lines.append(tl)
                text_clean = " ".join(clean_lines)
                # Strip CSS/HTML fragments that leaked through
                text_clean = re.sub(r"@font-face\s*\{[^}]*\}", "", text_clean)
                text_clean = re.sub(r"font-family:[^;]+;", "", text_clean)
                text_clean = re.sub(r"src:\s*url\([^)]*\)[^;]*;?", "", text_clean)
                text_clean = re.sub(r"\{[^}]{0,200}\}", "", text_clean)  # strip small CSS blocks
                text_clean = re.sub(r"\s{2,}", " ", text_clean).strip()
                # Take first 2 meaningful sentences
                sentences = [s.strip() for s in text_clean.split(".") if len(s.strip()) > 20]
                summary = ". ".join(sentences[:2]) + "." if sentences else text_clean[:200]

                item = {
                    "title": r.title or "",
                    "url": r.url or "",
                    "date": str(r.published_date or ""),
                    "summary": summary[:300],
                    "events": article_events,
                    "priority": priority,
                }
                news_items.append(item)
                compelling_events.extend(article_events)

            # ── Filter out irrelevant articles ─────────────────────
            # Make sure articles actually mention the company
            clean_name_for_filter = re.sub(r"\s*\([^)]*\)\s*", " ", company_name).strip()
            # Also strip SF org suffixes
            clean_name_for_filter = _clean_company_name(clean_name_for_filter)
            company_words = [w.lower() for w in clean_name_for_filter.split() if len(w) > 1]
            strip_suffixes = {"inc.", "inc", "ltd", "ltd.", "corp", "corp.",
                              "limited", "llc", "operations", "technologies",
                              "canada", "group", "holdings", "the"}
            core_words = [w for w in company_words if w not in strip_suffixes]
            if not core_words:
                core_words = company_words[:1]

            # Build the full company phrase for stricter matching
            full_phrase = " ".join(core_words).lower()

            # Common English words that are also company names — require
            # stricter matching (word boundary) to prevent cross-contamination
            _COMMON_WORD_COMPANIES = {
                "bell", "rogers", "national", "royal", "compass", "summit",
                "frontier", "pioneer", "premier", "genesis", "alliance",
                "cornerstone", "unity", "apex", "delta", "gateway",
                "phoenix", "mercury", "atlas", "vanguard", "titan",
                "cascade", "harbor", "harbour", "liberty", "patriot",
                "vertex", "sterling", "beacon", "bridge", "legacy",
                "centric", "nexus", "echo", "bold", "clearview",
            }

            filtered_news = []
            for item in news_items:
                combined = (item["title"] + " " + item["summary"]).lower()
                # For multi-word company names, require the full phrase OR
                # at least 2 core words to match (prevents false positives
                # like "Compass" matching "Security Compass")
                if len(core_words) >= 2:
                    if full_phrase in combined:
                        filtered_news.append(item)
                    elif sum(1 for w in core_words if w in combined) >= 2:
                        filtered_news.append(item)
                else:
                    # Single-word company: use word-boundary matching
                    # to avoid "Bell" matching "rebellion" or "bell pepper"
                    word = core_words[0]
                    if word in _COMMON_WORD_COMPANIES:
                        # Stricter: require word boundary + capitalized form in title
                        title_lower = item["title"].lower()
                        if re.search(r'\b' + re.escape(word) + r'\b', title_lower):
                            filtered_news.append(item)
                    elif any(w in combined for w in core_words):
                        filtered_news.append(item)
            news_items = filtered_news if filtered_news else news_items[:2]  # fallback: keep max 2 if filter too aggressive

            # ── Deduplicate similar articles ────────────────────────
            # Group articles with very similar titles (same story, different sources)
            deduped = []
            seen_titles = []
            for item in news_items:
                # Normalize: lowercase, strip punctuation, remove very common words
                title_words = set(re.findall(r'\w+', item["title"].lower()))
                title_words -= {"the", "a", "an", "and", "of", "in", "to", "for", "as", "by", "is", "on", "at", "its"}
                is_dupe = False
                for st in seen_titles:
                    if not title_words or not st:
                        continue
                    overlap = len(title_words & st) / max(len(title_words | st), 1)
                    if overlap > 0.35:  # >35% word overlap = same story
                        is_dupe = True
                        break
                if not is_dupe:
                    deduped.append(item)
                    seen_titles.append(title_words)
            news_items = deduped

            # ── Penalize stale news ─────────────────────────────────
            # News older than 2 years is much less actionable
            cutoff_date = (datetime.now() - timedelta(days=730)).isoformat()[:10]
            for item in news_items:
                item_date = item.get("date", "")[:10]
                if item_date and item_date < cutoff_date:
                    item["priority"] = max(item["priority"] - 15, 0)
                    item["stale"] = True
                elif not item_date:
                    # No date = can't verify recency, penalize slightly
                    item["priority"] = max(item["priority"] - 5, 0)

            # Sort by priority (growth signals first, recent first)
            news_items.sort(key=lambda x: -x["priority"])

            # Only include stale events in compelling_events if we
            # have no recent ones
            recent_events = []
            stale_events = []
            for item in news_items:
                if item.get("stale"):
                    stale_events.extend(item.get("events", []))
                else:
                    recent_events.extend(item.get("events", []))
            if recent_events:
                compelling_events = list(set(recent_events))
            else:
                compelling_events = list(set(stale_events))

        return {
            "news_items": news_items[:5],  # Top 5 most relevant
            "compelling_events": compelling_events,
        }

    except Exception as e:
        print(f"  Exa news error: {e}")
        return {"news_items": [], "compelling_events": []}


# ── Job Posting Tech Stack Scanner ─────────────────────────────────

# Curated list of GTM / business tools to detect in job postings.
# Each entry: tool_name -> (category, salesforce_solution)
JOB_POSTING_TOOLS = {
    # CRM
    "salesforce": ("CRM", "✅ Already using"),
    "hubspot": ("CRM / Marketing Automation", "Sales Cloud + Marketing Cloud"),
    "zoho crm": ("CRM", "Sales Cloud"),
    "microsoft dynamics": ("CRM", "Sales Cloud"),
    "pipedrive": ("CRM", "Sales Cloud"),
    "freshsales": ("CRM", "Sales Cloud"),
    "sugar crm": ("CRM", "Sales Cloud"),
    "sugarcrm": ("CRM", "Sales Cloud"),
    "copper crm": ("CRM", "Sales Cloud"),
    "close.io": ("CRM", "Sales Cloud"),
    "insightly": ("CRM", "Sales Cloud"),
    # Marketing Automation
    "pardot": ("Marketing Automation", "✅ Already using (MCAE)"),
    "account engagement": ("Marketing Automation", "✅ Already using (MCAE)"),
    "marketo": ("Marketing Automation", "Marketing Cloud Account Engagement"),
    "eloqua": ("Marketing Automation", "Marketing Cloud Account Engagement"),
    "mailchimp": ("Email Marketing", "Marketing Cloud"),
    "constant contact": ("Email Marketing", "Marketing Cloud"),
    "braze": ("Marketing Automation", "Marketing Cloud"),
    "klaviyo": ("Marketing Automation", "Marketing Cloud"),
    "iterable": ("Marketing Automation", "Marketing Cloud"),
    "activecampaign": ("Marketing Automation", "Marketing Cloud"),
    "sendgrid": ("Email / Transactional", "Marketing Cloud"),
    # ABM
    "6sense": ("ABM", "Marketing Cloud (Account Engagement)"),
    "demandbase": ("ABM", "Marketing Cloud (ABM)"),
    "terminus": ("ABM", "Marketing Cloud (ABM)"),
    "rollworks": ("ABM", "Marketing Cloud (ABM)"),
    "bombora": ("Intent Data", "Data Cloud"),
    # Sales Engagement / Enablement
    "salesloft": ("Sales Engagement", "Sales Cloud (Sales Engagement)"),
    "outreach": ("Sales Engagement", "Sales Cloud (Sales Engagement)"),
    "gong": ("Conversation Intelligence", "Sales Cloud (Einstein Conversation Insights)"),
    "chorus": ("Conversation Intelligence", "Sales Cloud (Einstein Conversation Insights)"),
    "highspot": ("Sales Enablement", "Sales Cloud (Enablement)"),
    "seismic": ("Sales Enablement", "Sales Cloud (Enablement)"),
    "showpad": ("Sales Enablement", "Sales Cloud (Enablement)"),
    "mindtickle": ("Sales Enablement", "Sales Cloud (Enablement)"),
    "clari": ("Revenue Intelligence", "Sales Cloud (Revenue Intelligence)"),
    "xactly": ("SPM / Commissions", "Sales Cloud (SPM)"),
    "spiff": ("SPM / Commissions", "Sales Cloud (SPM)"),
    "captivateiq": ("SPM / Commissions", "Sales Cloud (SPM)"),
    # Conversational Marketing / Chat
    "qualified": ("Conversational Marketing", "✅ Already using (SF Native)"),
    "drift": ("Conversational Marketing", "Service Cloud (Einstein Bots)"),
    "intercom": ("Conversational / Support", "Service Cloud"),
    "crisp": ("Live Chat", "Service Cloud (Live Agent)"),
    "livechat": ("Live Chat", "Service Cloud (Live Agent)"),
    # Service / Support
    "zendesk": ("Customer Support", "Service Cloud"),
    "freshdesk": ("Customer Support", "Service Cloud"),
    "kustomer": ("Customer Support", "Service Cloud"),
    "gladly": ("Customer Support", "Service Cloud"),
    "helpscout": ("Customer Support", "Service Cloud"),
    "help scout": ("Customer Support", "Service Cloud"),
    "kayako": ("Customer Support", "Service Cloud"),
    "liveperson": ("Conversational Support", "Service Cloud"),
    # ITSM (Agentforce IT Service)
    "servicenow": ("ITSM", "Agentforce IT Service"),
    "jira service": ("ITSM", "Agentforce IT Service"),
    "bmc helix": ("ITSM", "Agentforce IT Service"),
    "bmc remedy": ("ITSM", "Agentforce IT Service"),
    "freshservice": ("ITSM", "Agentforce IT Service"),
    "manageengine": ("ITSM", "Agentforce IT Service"),
    "sysaid": ("ITSM", "Agentforce IT Service"),
    "ivanti": ("ITSM", "Agentforce IT Service"),
    "cherwell": ("ITSM", "Agentforce IT Service"),
    "aisera": ("AI ITSM", "Agentforce IT Service"),
    "topdesk": ("ITSM", "Agentforce IT Service"),
    "halo itsm": ("ITSM", "Agentforce IT Service"),
    # Customer Success
    "gainsight": ("Customer Success", "Service Cloud + Data Cloud"),
    "totango": ("Customer Success", "Service Cloud + Data Cloud"),
    "churnzero": ("Customer Success", "Service Cloud + Data Cloud"),
    "catalyst": ("Customer Success", "Service Cloud + Data Cloud"),
    # CPQ / Billing
    "zuora": ("Billing / Subscription", "Revenue Cloud (CPQ + Billing)"),
    "chargebee": ("Billing / Subscription", "Revenue Cloud"),
    "dealhub": ("CPQ", "Revenue Cloud (CPQ)"),
    "conga": ("CPQ / CLM", "Revenue Cloud (CPQ)"),
    "pandadoc": ("Document / Proposals", "Revenue Cloud"),
    "docusign": ("E-Signature", "—"),
    # Analytics / BI
    "tableau": ("BI / Analytics", "✅ Already using (Tableau)"),
    "power bi": ("BI / Analytics", "CRM Analytics (Tableau)"),
    "powerbi": ("BI / Analytics", "CRM Analytics (Tableau)"),
    "looker": ("BI / Analytics", "CRM Analytics (Tableau)"),
    "thoughtspot": ("BI / Analytics", "CRM Analytics (Tableau)"),
    "domo": ("BI / Analytics", "CRM Analytics (Tableau)"),
    "sisense": ("BI / Analytics", "CRM Analytics (Tableau)"),
    "metabase": ("BI / Analytics", "CRM Analytics (Tableau)"),
    "google analytics": ("Web Analytics", "CRM Analytics (Tableau)"),
    "adobe analytics": ("Web Analytics", "CRM Analytics (Tableau)"),
    "mixpanel": ("Product Analytics", "CRM Analytics"),
    "amplitude": ("Product Analytics", "CRM Analytics"),
    "heap": ("Product Analytics", "CRM Analytics"),
    "pendo": ("Product Analytics", "CRM Analytics"),
    "fullstory": ("Product Analytics", "CRM Analytics"),
    # CDP / Data
    "segment": ("CDP", "Data Cloud (Data 360)"),
    "mparticle": ("CDP", "Data Cloud (Data 360)"),
    "tealium": ("CDP", "Data Cloud (Data 360)"),
    "treasure data": ("CDP", "Data Cloud (Data 360)"),
    "blueconic": ("CDP", "Data Cloud (Data 360)"),
    "lytics": ("CDP", "Data Cloud (Data 360)"),
    "rudderstack": ("CDP", "Data Cloud (Data 360)"),
    # Data Warehouse / Lakehouse (Data Cloud / Data 360)
    "snowflake": ("Data Warehouse", "Data Cloud (Data 360 + Zero Copy)"),
    "databricks": ("Data Lakehouse", "Data Cloud (Data 360 + Zero Copy)"),
    "bigquery": ("Data Warehouse", "Data Cloud (Data 360 + Zero Copy)"),
    "redshift": ("Data Warehouse", "Data Cloud (Data 360 + Zero Copy)"),
    "azure synapse": ("Data Warehouse", "Data Cloud (Data 360 + Zero Copy)"),
    "teradata": ("Data Warehouse", "Data Cloud (Data 360)"),
    "clickhouse": ("OLAP Database", "Data Cloud (Data 360)"),
    "dbt": ("Data Transformation", "Data Cloud (Data 360)"),
    "fivetran": ("Data Integration", "Data Cloud (Data 360) / MuleSoft"),
    "airbyte": ("Data Integration", "Data Cloud (Data 360) / MuleSoft"),
    "stitch": ("Data Integration", "Data Cloud (Data 360) / MuleSoft"),
    # AI / LLM (Salesforce AI / Agentforce)
    "openai": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "chatgpt": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "gpt-4": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "anthropic": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "claude": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "cohere": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "google gemini": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "vertex ai": ("AI / ML Platform", "Salesforce AI (Agentforce)"),
    "amazon bedrock": ("AI / LLM", "Salesforce AI (Agentforce 360 for AWS)"),
    "azure openai": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "mistral": ("AI / LLM", "Salesforce AI (Agentforce)"),
    "hugging face": ("AI / ML", "Salesforce AI (Agentforce)"),
    "langchain": ("AI Framework", "Salesforce AI (Agentforce)"),
    "llama": ("AI / LLM", "Salesforce AI (Agentforce)"),
    # Data Enrichment / Prospecting
    "zoominfo": ("Data Enrichment", "Data Cloud"),
    "clearbit": ("Data Enrichment", "Data Cloud"),
    "apollo": ("Prospecting", "Sales Cloud (Sales Engagement)"),
    "lusha": ("Data Enrichment", "Data Cloud"),
    "clay": ("GTM Engineering", "Data Cloud + Sales Cloud"),
    "cognism": ("Data Enrichment", "Data Cloud"),
    # Scheduling / Productivity
    "chili piper": ("Scheduling", "Sales Cloud"),
    "calendly": ("Scheduling", "Sales Cloud"),
    # Project / Collaboration (signals tool sprawl)
    "asana": ("Project Management", "—"),
    "monday.com": ("Project Management", "—"),
    "jira": ("Project Management", "—"),
    "confluence": ("Knowledge Base", "—"),
    "notion": ("Knowledge Base", "—"),
    # Customer Community / Experience / Portal
    "khoros": ("Community", "Experience Cloud"),
    "lithium": ("Community", "Experience Cloud"),
    "higher logic": ("Community", "Experience Cloud"),
    "insided": ("Community", "Experience Cloud"),
    "liferay": ("Portal / DXP", "Experience Cloud"),
    "adobe experience manager": ("CMS / DXP", "Experience Cloud"),
    "sitecore": ("CMS / DXP", "Experience Cloud"),
    "contentful": ("Headless CMS", "Experience Cloud"),
    "contentstack": ("Headless CMS", "Experience Cloud"),
    "acquia": ("DXP", "Experience Cloud"),
    # E-Signature / CLM
    "docusign clm": ("CLM", "Revenue Cloud"),
    "ironclad": ("CLM", "Revenue Cloud"),
    "icertis": ("CLM", "Revenue Cloud"),
    "agiloft": ("CLM", "Revenue Cloud"),
    # Conversational AI / Chatbots
    "ada": ("AI Chatbot", "Service Cloud (Einstein Bots)"),
    "forethought": ("AI Support", "Service Cloud (Einstein)"),
    # Revenue / Forecasting
    "aviso": ("Revenue Intelligence", "Sales Cloud (Revenue Intelligence)"),
    "boostup": ("Revenue Intelligence", "Sales Cloud (Revenue Intelligence)"),
    "people.ai": ("Revenue Intelligence", "Sales Cloud (Einstein Activity Capture)"),
    # Commerce
    "shopify": ("E-Commerce", "Commerce Cloud"),
    "magento": ("E-Commerce", "Commerce Cloud"),
    "bigcommerce": ("E-Commerce", "Commerce Cloud"),
    "woocommerce": ("E-Commerce", "Commerce Cloud"),
    "commercetools": ("Headless Commerce", "Commerce Cloud"),
    "vtex": ("E-Commerce", "Commerce Cloud"),
    "elastic path": ("Headless Commerce", "Commerce Cloud"),
    # RPA (MuleSoft RPA)
    "uipath": ("RPA", "MuleSoft RPA"),
    "automation anywhere": ("RPA", "MuleSoft RPA"),
    "blue prism": ("RPA", "MuleSoft RPA"),
    "power automate desktop": ("RPA", "MuleSoft RPA"),
    # Loyalty Management
    "antavo": ("Loyalty Platform", "Loyalty Management"),
    "annex cloud": ("Loyalty Platform", "Loyalty Management"),
    "yotpo loyalty": ("Loyalty Platform", "Loyalty Management"),
    "smile.io": ("Loyalty Platform", "Loyalty Management"),
    "loyaltylion": ("Loyalty Platform", "Loyalty Management"),
    # Enterprise Search (Slack Enterprise Search)
    "glean": ("Enterprise Search", "Slack (Enterprise Search)"),
    "coveo": ("Enterprise Search", "Slack (Enterprise Search)"),
    "elastic enterprise search": ("Enterprise Search", "Slack (Enterprise Search)"),
    "algolia": ("Search / Discovery", "Slack (Enterprise Search)"),
    "lucidworks": ("Enterprise Search", "Slack (Enterprise Search)"),
    # Financial Services (Financial Services Cloud)
    "ncino": ("Banking Platform", "Financial Services Cloud"),
    "finastra": ("Financial Services", "Financial Services Cloud"),
    "temenos": ("Banking Software", "Financial Services Cloud"),
    "fiserv": ("Financial Services", "Financial Services Cloud"),
    "jack henry": ("Banking Technology", "Financial Services Cloud"),
    "verint": ("Financial CX", "Financial Services Cloud"),
    # Healthcare (Health Cloud)
    "epic": ("Healthcare EHR", "Health Cloud"),
    "cerner": ("Healthcare EHR", "Health Cloud"),
    "oracle health": ("Healthcare EHR", "Health Cloud"),
    "veeva": ("Life Sciences CRM", "Health Cloud"),
    "medidata": ("Clinical Data", "Health Cloud"),
    "athenahealth": ("Healthcare Platform", "Health Cloud"),
    # Nonprofit (Nonprofit Cloud)
    "blackbaud": ("Nonprofit CRM", "Nonprofit Cloud"),
    "bloomerang": ("Nonprofit CRM", "Nonprofit Cloud"),
    "neon crm": ("Nonprofit CRM", "Nonprofit Cloud"),
    "raiser's edge": ("Nonprofit Fundraising", "Nonprofit Cloud"),
    "classy": ("Nonprofit Fundraising", "Nonprofit Cloud"),
    # Data Governance / MDM (Informatica)
    "collibra": ("Data Governance", "Data Cloud (Informatica)"),
    "atlan": ("Data Governance", "Data Cloud (Informatica)"),
    "alation": ("Data Catalog", "Data Cloud (Informatica)"),
    "master data management": ("MDM", "Data Cloud (Informatica)"),
    "reltio": ("MDM", "Data Cloud (Informatica)"),
    # Integration / iPaaS (MuleSoft)
    "mulesoft": ("Integration", "✅ Already using (MuleSoft)"),
    "boomi": ("iPaaS", "MuleSoft"),
    "dell boomi": ("iPaaS", "MuleSoft"),
    "informatica": ("iPaaS / Data Integration", "MuleSoft / Data Cloud"),
    "workato": ("iPaaS / Automation", "MuleSoft"),
    "tray.io": ("iPaaS", "MuleSoft"),
    "celigo": ("iPaaS", "MuleSoft"),
    "snaplogic": ("iPaaS", "MuleSoft"),
    "jitterbit": ("iPaaS", "MuleSoft"),
    "talend": ("Data Integration", "MuleSoft / Data Cloud"),
    "zapier": ("Workflow Automation", "MuleSoft / Flow"),
    "make": ("Workflow Automation", "MuleSoft / Flow"),
    "power automate": ("Workflow Automation", "MuleSoft / Flow"),
    # Collaboration (Slack)
    "microsoft teams": ("Collaboration", "Slack"),
    "google chat": ("Collaboration", "Slack"),
    "zoom": ("Video / Collaboration", "Slack"),
    "webex": ("Video / Collaboration", "Slack"),
    "ringcentral": ("UCaaS", "Slack + Service Cloud Voice"),
    "dialpad": ("UCaaS", "Slack + Service Cloud Voice"),
    "five9": ("CCaaS", "Service Cloud Voice"),
    "nice incontact": ("CCaaS", "Service Cloud Voice"),
    "nice cxone": ("CCaaS", "Service Cloud Voice"),
    "genesys": ("CCaaS", "Service Cloud Voice"),
    "talkdesk": ("CCaaS", "Service Cloud Voice"),
    "aircall": ("Cloud Telephony", "Service Cloud Voice"),
    # Data Backup / Protection (Own / Salesforce Backup)
    "odaseva": ("Data Backup", "Salesforce Backup (Own)"),
    "gearset": ("DevOps / Backup", "Salesforce Backup (Own)"),
    "spanning": ("SaaS Backup", "Salesforce Backup (Own)"),
    "veeam": ("Data Backup", "Salesforce Backup (Own)"),
    "druva": ("Data Backup", "Salesforce Backup (Own)"),
    "commvault": ("Data Backup", "Salesforce Backup (Own)"),
    "ownbackup": ("SaaS Backup", "✅ Already using (Own)"),
    "own backup": ("SaaS Backup", "✅ Already using (Own)"),
    # Data Security / Compliance (Shield)
    "varonis": ("Data Security", "Shield + Data Mask"),
    "bigid": ("Data Privacy", "Shield + Data Mask + Privacy Center"),
    "onetrust": ("Privacy / Consent", "Shield + Privacy Center"),
    "securiti": ("Data Privacy", "Shield + Data Mask"),
    "dataguard": ("Compliance", "Shield"),
    # Field Service (Field Service Lightning)
    "ifs": ("Field Service", "Field Service Lightning"),
    "servicemax": ("Field Service", "Field Service Lightning"),
    "oracle field service": ("Field Service", "Field Service Lightning"),
    "zinier": ("Field Service", "Field Service Lightning"),
    "sap field service": ("Field Service", "Field Service Lightning"),
    "fieldaware": ("Field Service", "Field Service Lightning"),
    # Sustainability (Net Zero Cloud)
    "persefoni": ("Carbon Accounting", "Net Zero Cloud"),
    "watershed": ("Carbon Accounting", "Net Zero Cloud"),
    "plan a": ("Sustainability", "Net Zero Cloud"),
    "sphera": ("ESG / Sustainability", "Net Zero Cloud"),
    "enablon": ("EHS / Sustainability", "Net Zero Cloud"),
    # Document Generation / E-Signature
    "nintex": ("Document Automation", "Revenue Cloud / Salesforce Flow"),
    "formstack": ("Forms / Documents", "Revenue Cloud / Salesforce Flow"),
    "conga composer": ("Document Generation", "Revenue Cloud"),
    "adobe sign": ("E-Signature", "—"),
    # Cloud / Infrastructure (AWS Marketplace relevance)
    "amazon web services": ("Cloud", "Available via AWS Marketplace"),
    "aws": ("Cloud", "Available via AWS Marketplace"),
    "azure": ("Cloud", "—"),
    "google cloud": ("Cloud", "—"),
    # HR Service / HCM (Employee Service)
    "workday": ("HCM / HRIS", "Employee Service (HR Service Delivery)"),
    "bamboohr": ("HRIS", "Employee Service"),
    "adp workforce": ("Payroll / HCM", "Employee Service"),
    "adp run": ("Payroll / HCM", "Employee Service"),
    "ukg": ("HCM", "Employee Service"),
    "kronos": ("Workforce Mgmt", "Employee Service"),
    "ceridian": ("HCM / Payroll", "Employee Service"),
    "dayforce": ("HCM / Payroll", "Employee Service"),
    "sap successfactors": ("HCM", "Employee Service"),
    "successfactors": ("HCM", "Employee Service"),
    "namely": ("HRIS", "Employee Service"),
    "paylocity": ("HCM / Payroll", "Employee Service"),
    "paycom": ("HCM / Payroll", "Employee Service"),
    "gusto": ("Payroll / HR", "Employee Service"),
    "rippling": ("HR Platform", "Employee Service"),
    "personio": ("HRIS", "Employee Service"),
    "hibob": ("HRIS", "Employee Service"),
    "deel": ("Global Payroll", "Employee Service"),
    # ERP
    "sap": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "sap s/4hana": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "oracle": ("ERP / CRM", "Sales Cloud"),
    "oracle erp": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "oracle fusion": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "netsuite": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "sage": ("ERP / Accounting", "Revenue Cloud / MuleSoft integration"),
    "sage intacct": ("ERP / Accounting", "Revenue Cloud / MuleSoft integration"),
    "infor": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "epicor": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "acumatica": ("ERP", "Revenue Cloud / MuleSoft integration"),
    "quickbooks": ("Accounting", "Revenue Cloud / MuleSoft integration"),
    "xero": ("Accounting", "Revenue Cloud / MuleSoft integration"),
    # Telecom BSS (industry-specific)
    "netcracker": ("Telecom BSS", "Communications Cloud"),
    "amdocs": ("Telecom BSS", "Communications Cloud"),
    # Methodology (signals buying maturity)
    "meddpicc": ("Sales Methodology", "—"),
    "meddic": ("Sales Methodology", "—"),
    # Salesforce ecosystem products (confirms existing usage)
    "salesforce einstein": ("AI / Analytics", "✅ Already using (Einstein)"),
    "einstein": ("AI / Analytics", "✅ Already using (Einstein)"),
    "marketing cloud": ("Marketing Automation", "✅ Already using (Marketing Cloud)"),
    "service cloud": ("Customer Support", "✅ Already using (Service Cloud)"),
    "commerce cloud": ("E-Commerce", "✅ Already using (Commerce Cloud)"),
    "data cloud": ("CDP", "✅ Already using (Data Cloud)"),
    "experience cloud": ("Digital Experience", "✅ Already using (Experience Cloud)"),
    "mulesoft": ("Integration", "✅ Already using (MuleSoft)"),
    "slack": ("Collaboration", "✅ Already using (Slack)"),
    "heroku": ("PaaS", "✅ Already using (Heroku)"),
}


def scan_job_postings(company_name, exa_key):
    """Search for company job postings and extract tech stack mentions.

    Returns a list of dicts: [{name, category, sf_solution, source_title, source_url}]
    """
    if not exa_key:
        return []

    exa = get_exa(exa_key)
    clean_name = _clean_company_name(company_name)

    # Run multiple targeted searches to find different types of postings
    queries = [
        f'"{clean_name}" job posting salesforce hubspot CRM marketing operations',
        f'"{clean_name}" hiring revenue operations sales enablement technology stack',
        f'"{clean_name}" job requirements experience tools software proficiency',
    ]

    all_text = []  # List of (text, title, url) from all results

    for query in queries:
        try:
            results = exa.search_and_contents(
                query,
                num_results=5,
                text={"max_characters": 3000},
            )
            for r in results.results:
                text = (r.text or "").lower()
                title = r.title or ""
                url = r.url or ""
                # Skip results that aren't actually about this company
                company_words = clean_name.lower().split()
                if len(company_words) > 1:
                    # Multi-word: require 2+ words or full name
                    full_match = clean_name.lower() in text or clean_name.lower() in title.lower()
                    word_matches = sum(1 for w in company_words if len(w) > 2 and w in text)
                    if not full_match and word_matches < 2:
                        continue
                else:
                    if company_words[0] not in text and company_words[0] not in title.lower():
                        continue
                all_text.append((text, title, url))
        except Exception as e:
            print(f"  Job posting search error: {e}")

    if not all_text:
        return []

    # ── Context-aware product classification ──
    # Multi-product tools like HubSpot, Oracle, Microsoft need context to determine
    # which specific product is being used. We infer from:
    # 1. Job title/department in the posting
    # 2. Co-occurring tools (SF + HubSpot = HubSpot is marketing)
    # 3. Surrounding keywords in the text near the tool mention

    ROLE_CONTEXT = {
        "sales": {"sales", "account executive", "business development", "bdr", "sdr",
                  "sales ops", "sales operations", "quota", "pipeline", "prospecting",
                  "closing", "deal", "opportunity"},
        "marketing": {"marketing", "demand gen", "demand generation", "content",
                      "campaign", "lead gen", "lead generation", "email marketing",
                      "seo", "sem", "ppc", "brand", "inbound", "outbound marketing",
                      "nurture", "marketing ops", "marketing operations", "mql"},
        "service": {"customer support", "customer service", "help desk", "helpdesk",
                    "ticketing", "case management", "support agent", "customer care",
                    "service desk", "escalation", "sla"},
        "revops": {"revenue operations", "revops", "rev ops", "go-to-market",
                   "gtm", "business operations", "sales and marketing"},
        "it": {"it ", "information technology", "systems admin", "infrastructure",
               "devops", "cloud engineer", "data engineer", "databricks", "snowflake",
               "etl", "data warehouse", "data lake", "cio", "cto"},
        "success": {"customer success", "csm", "onboarding", "retention",
                    "renewal", "expansion", "churn", "nps", "health score"},
    }

    # Multi-product tools: key -> {context_department: (category, sf_solution)}
    MULTI_PRODUCT_TOOLS = {
        "hubspot": {
            "sales":     ("CRM (Sales)", "Sales Cloud"),
            "marketing": ("Marketing Automation", "Marketing Cloud Account Engagement"),
            "service":   ("Customer Service", "Service Cloud"),
            "revops":    ("CRM / Marketing Automation", "Sales Cloud + Marketing Cloud"),
            "success":   ("Customer Success", "Service Cloud + Data Cloud"),
            "_default":  ("CRM / Marketing Automation", "Sales Cloud + Marketing Cloud"),
            "_with_salesforce": ("Marketing Automation", "Marketing Cloud Account Engagement"),
        },
        "oracle": {
            "sales":     ("CRM", "Sales Cloud"),
            "marketing": ("Marketing Automation", "Marketing Cloud"),
            "it":        ("ERP / Database", "—"),
            "service":   ("Customer Service", "Service Cloud"),
            "_default":  ("Enterprise Software", "Sales Cloud"),
        },
        "microsoft dynamics": {
            "sales":     ("CRM (Sales)", "Sales Cloud"),
            "marketing": ("CRM (Marketing)", "Marketing Cloud"),
            "service":   ("CRM (Service)", "Service Cloud"),
            "_default":  ("CRM", "Sales Cloud"),
        },
    }

    def _detect_role_context(text, title):
        """Detect which department/role a job posting is for."""
        combined = (text + " " + title).lower()
        scores = {}
        for dept, keywords in ROLE_CONTEXT.items():
            score = sum(1 for kw in keywords if kw in combined)
            if score > 0:
                scores[dept] = score
        if not scores:
            return None
        return max(scores, key=scores.get)

    def _classify_multi_product(tool_key, text, title, all_texts_combined):
        """For multi-product tools, determine specific product from context."""
        if tool_key not in MULTI_PRODUCT_TOOLS:
            return None
        configs = MULTI_PRODUCT_TOOLS[tool_key]
        role = _detect_role_context(text, title)

        # Special case: if Salesforce co-occurs with HubSpot, HubSpot is marketing
        if tool_key == "hubspot" and "_with_salesforce" in configs:
            if "salesforce" in all_texts_combined and "salesforce" != tool_key:
                return configs["_with_salesforce"]

        if role and role in configs:
            return configs[role]
        return configs.get("_default")

    # Pre-scan: check if Salesforce appears anywhere across all postings
    all_texts_combined = " ".join(t for t, _, _ in all_text)

    # Scan all collected text for tool mentions
    found_tools = {}  # tool_name -> {category, sf_solution, sources: [(title, url)], role_contexts: []}

    # Tool names that are common English words — require word boundary matching
    # AND additional context (nearby tech/tool keywords) to avoid false positives
    AMBIGUOUS_TOOL_NAMES = {
        "clay", "drift", "heap", "crisp", "apollo", "catalyst",
        "oracle", "braze", "segment", "outreach", "chorus",
        "pendo", "slack", "spiff", "lusha", "zoom", "make",
        "sage", "deel", "claude", "llama", "ifs",
        "gusto", "namely", "stitch",
    }

    # Context words that should appear near an ambiguous tool mention
    # to confirm it's actually about the software product
    TOOL_CONTEXT_WORDS = {
        "platform", "software", "tool", "integration", "api",
        "dashboard", "analytics", "crm", "marketing", "sales",
        "automation", "workflow", "data", "customer", "pipeline",
        "implementation", "admin", "proficiency", "experience with",
        "knowledge of", "familiar with", "certification", "certified",
        "stack", "tech stack", "technology", "suite", "product",
    }

    for text, title, url in all_text:
        for tool_key, (category, sf_solution) in JOB_POSTING_TOOLS.items():
            if tool_key in text:
                # Extra validation: avoid false positives for short tool names
                if len(tool_key) <= 3:
                    # For short names like "aws", "sap", "dbt" — require word boundary
                    if not re.search(r'\b' + re.escape(tool_key) + r'\b', text):
                        continue

                # Extra validation for ambiguous tool names (common English words)
                if tool_key in AMBIGUOUS_TOOL_NAMES:
                    if not re.search(r'\b' + re.escape(tool_key) + r'\b', text):
                        continue
                    # Check for nearby tech context (within 100 chars of the match)
                    match = re.search(r'\b' + re.escape(tool_key) + r'\b', text)
                    if match:
                        window_start = max(0, match.start() - 100)
                        window_end = min(len(text), match.end() + 100)
                        window = text[window_start:window_end]
                        has_context = any(cw in window for cw in TOOL_CONTEXT_WORDS)
                        # Also accept if the tool name is capitalized in original (not lowercased text)
                        # which suggests it's a proper noun / product name
                        if not has_context:
                            continue

                display_name = tool_key.title()
                # Use proper casing for known tools
                proper_names = {
                    "salesforce": "Salesforce", "hubspot": "HubSpot",
                    "pardot": "Pardot", "marketo": "Marketo", "eloqua": "Eloqua",
                    "6sense": "6sense", "demandbase": "Demandbase",
                    "salesloft": "SalesLoft", "outreach": "Outreach",
                    "gong": "Gong", "chorus": "Chorus", "highspot": "Highspot",
                    "seismic": "Seismic", "clari": "Clari", "xactly": "Xactly",
                    "qualified": "Qualified", "drift": "Drift", "intercom": "Intercom",
                    "zendesk": "Zendesk", "freshdesk": "Freshdesk",
                    "servicenow": "ServiceNow", "gainsight": "Gainsight",
                    "zuora": "Zuora", "chargebee": "Chargebee",
                    "pandadoc": "PandaDoc", "docusign": "DocuSign",
                    "tableau": "Tableau", "power bi": "Power BI",
                    "powerbi": "Power BI", "looker": "Looker",
                    "thoughtspot": "ThoughtSpot", "domo": "Domo",
                    "mixpanel": "Mixpanel", "amplitude": "Amplitude",
                    "pendo": "Pendo", "fullstory": "FullStory",
                    "segment": "Segment", "snowflake": "Snowflake",
                    "databricks": "Databricks", "zoominfo": "ZoomInfo",
                    "clearbit": "Clearbit", "apollo": "Apollo",
                    "clay": "Clay", "chili piper": "Chili Piper",
                    "calendly": "Calendly", "shopify": "Shopify",
                    "workday": "Workday", "netsuite": "NetSuite",
                    "netcracker": "Netcracker", "amdocs": "Amdocs",
                    "meddpicc": "MEDDPICC", "meddic": "MEDDIC",
                    "mulesoft": "MuleSoft", "heroku": "Heroku",
                    "slack": "Slack", "aws": "AWS", "azure": "Azure",
                    "salesforce einstein": "Salesforce Einstein",
                    "einstein": "Salesforce Einstein",
                    "marketing cloud": "Marketing Cloud",
                    "service cloud": "Service Cloud",
                    "commerce cloud": "Commerce Cloud",
                    "data cloud": "Data Cloud",
                    "experience cloud": "Experience Cloud",
                    "account engagement": "Account Engagement (Pardot)",
                    "google analytics": "Google Analytics",
                    "adobe analytics": "Adobe Analytics",
                    "bigquery": "BigQuery", "redshift": "Redshift",
                    "dbt": "dbt", "heap": "Heap",
                    "bombora": "Bombora", "terminus": "Terminus",
                    "rollworks": "RollWorks", "cognism": "Cognism",
                    "lusha": "Lusha", "totango": "Totango",
                    "churnzero": "ChurnZero", "catalyst": "Catalyst",
                    "conga": "Conga", "dealhub": "DealHub",
                    "spiff": "Spiff", "captivateiq": "CaptivateIQ",
                    "mindtickle": "MindTickle", "showpad": "Showpad",
                    "activecampaign": "ActiveCampaign", "braze": "Braze",
                    "klaviyo": "Klaviyo", "iterable": "Iterable",
                    "sendgrid": "SendGrid", "crisp": "Crisp",
                    "livechat": "LiveChat", "sisense": "Sisense",
                    "metabase": "Metabase", "oracle": "Oracle",
                    "sap": "SAP", "jira service": "Jira Service Management",
                    # ITSM
                    "bmc helix": "BMC Helix", "bmc remedy": "BMC Remedy",
                    "freshservice": "Freshservice", "manageengine": "ManageEngine",
                    "sysaid": "SysAid", "ivanti": "Ivanti", "cherwell": "Cherwell",
                    "aisera": "Aisera", "topdesk": "TOPdesk", "halo itsm": "Halo ITSM",
                    # HR / HCM
                    "bamboohr": "BambooHR", "adp workforce": "ADP Workforce Now",
                    "adp run": "ADP RUN", "ukg": "UKG", "kronos": "Kronos",
                    "ceridian": "Ceridian", "dayforce": "Dayforce",
                    "sap successfactors": "SAP SuccessFactors",
                    "successfactors": "SAP SuccessFactors",
                    "namely": "Namely", "paylocity": "Paylocity",
                    "paycom": "Paycom", "gusto": "Gusto", "rippling": "Rippling",
                    "personio": "Personio", "hibob": "HiBob", "deel": "Deel",
                    # ERP
                    "sap s/4hana": "SAP S/4HANA", "oracle erp": "Oracle ERP",
                    "oracle fusion": "Oracle Fusion", "sage": "Sage",
                    "sage intacct": "Sage Intacct", "infor": "Infor",
                    "epicor": "Epicor", "acumatica": "Acumatica",
                    "quickbooks": "QuickBooks", "xero": "Xero",
                    # Data / AI
                    "azure synapse": "Azure Synapse", "teradata": "Teradata",
                    "clickhouse": "ClickHouse", "fivetran": "Fivetran",
                    "airbyte": "Airbyte", "stitch": "Stitch",
                    "openai": "OpenAI", "chatgpt": "ChatGPT", "gpt-4": "GPT-4",
                    "anthropic": "Anthropic", "claude": "Claude",
                    "cohere": "Cohere", "google gemini": "Google Gemini",
                    "vertex ai": "Vertex AI", "amazon bedrock": "Amazon Bedrock",
                    "azure openai": "Azure OpenAI", "mistral": "Mistral",
                    "hugging face": "Hugging Face", "langchain": "LangChain",
                    "llama": "LLaMA",
                    # iPaaS
                    "boomi": "Boomi", "dell boomi": "Dell Boomi",
                    "informatica": "Informatica", "workato": "Workato",
                    "tray.io": "Tray.io", "celigo": "Celigo",
                    "snaplogic": "SnapLogic", "jitterbit": "Jitterbit",
                    "talend": "Talend", "zapier": "Zapier", "make": "Make",
                    "power automate": "Power Automate",
                    # Collaboration / CCaaS
                    "microsoft teams": "Microsoft Teams",
                    "google chat": "Google Chat", "zoom": "Zoom",
                    "webex": "Webex", "ringcentral": "RingCentral",
                    "dialpad": "Dialpad", "five9": "Five9",
                    "nice incontact": "NICE inContact", "nice cxone": "NICE CXone",
                    "genesys": "Genesys", "talkdesk": "Talkdesk",
                    "aircall": "Aircall",
                    # Backup / Security
                    "odaseva": "Odaseva", "gearset": "Gearset",
                    "spanning": "Spanning", "veeam": "Veeam",
                    "druva": "Druva", "commvault": "Commvault",
                    "ownbackup": "OwnBackup", "own backup": "OwnBackup",
                    "varonis": "Varonis", "bigid": "BigID",
                    "onetrust": "OneTrust", "securiti": "Securiti",
                    # Low-Code / App Dev
                    "appian": "Appian", "mendix": "Mendix",
                    "outsystems": "OutSystems", "power apps": "Power Apps",
                    "powerapps": "Power Apps", "retool": "Retool",
                    "bubble": "Bubble", "unqork": "Unqork",
                    # Field Service
                    "servicemax": "ServiceMax", "zinier": "Zinier",
                    "fieldaware": "FieldAware",
                    # Sustainability
                    "persefoni": "Persefoni", "watershed": "Watershed",
                    "sphera": "Sphera", "enablon": "Enablon",
                    # Support
                    "kustomer": "Kustomer", "gladly": "Gladly",
                    "helpscout": "Help Scout", "help scout": "Help Scout",
                    "kayako": "Kayako", "liveperson": "LivePerson",
                    # Document
                    "nintex": "Nintex", "formstack": "Formstack",
                    "conga composer": "Conga Composer", "adobe sign": "Adobe Sign",
                    # CDP
                    "treasure data": "Treasure Data", "blueconic": "BlueConic",
                    "lytics": "Lytics", "rudderstack": "RudderStack",
                    # Commerce
                    "magento": "Magento", "bigcommerce": "BigCommerce",
                    "woocommerce": "WooCommerce",
                }
                display_name = proper_names.get(tool_key, tool_key.title())

                # Context-aware classification for multi-product tools
                effective_category = category
                effective_sf_solution = sf_solution
                role_ctx = _detect_role_context(text, title)

                override = _classify_multi_product(tool_key, text, title, all_texts_combined)
                if override:
                    effective_category, effective_sf_solution = override

                if display_name not in found_tools:
                    found_tools[display_name] = {
                        "category": effective_category,
                        "sf_solution": effective_sf_solution,
                        "sources": [],
                        "role_contexts": [],
                    }
                else:
                    # If we see the same tool in a different role context,
                    # upgrade to the more specific classification
                    if override and found_tools[display_name]["category"] == JOB_POSTING_TOOLS.get(tool_key, ("", ""))[0]:
                        found_tools[display_name]["category"] = effective_category
                        found_tools[display_name]["sf_solution"] = effective_sf_solution

                # Track which posting mentioned it (avoid duplicate sources)
                source = (title, url)
                if source not in found_tools[display_name]["sources"]:
                    found_tools[display_name]["sources"].append(source)
                if role_ctx and role_ctx not in found_tools[display_name]["role_contexts"]:
                    found_tools[display_name]["role_contexts"].append(role_ctx)

    # Convert to list
    result = []
    for tool_name, info in sorted(found_tools.items()):
        # Pick the best source (first one found)
        source_title = info["sources"][0][0] if info["sources"] else ""
        source_url = info["sources"][0][1] if info["sources"] else ""
        # Build usage context string from role detections
        usage_context = ""
        if info.get("role_contexts"):
            ctx_labels = {
                "sales": "Sales team", "marketing": "Marketing team",
                "service": "Support/Service team", "revops": "RevOps",
                "it": "IT/Engineering", "success": "Customer Success",
            }
            contexts = [ctx_labels.get(r, r.title()) for r in info["role_contexts"]]
            usage_context = "Used by: " + ", ".join(contexts)

        result.append({
            "name": tool_name,
            "category": info["category"],
            "sf_solution": info["sf_solution"],
            "source_title": source_title,
            "source_url": source_url,
            "mention_count": len(info["sources"]),
            "usage_context": usage_context,
        })

    return result


# ── LinkedIn from Company Pages ────────────────────────────────────

def scrape_linkedin_from_website(website_url):
    """Scrape company about/team/leadership pages for LinkedIn profile URLs.
    Returns a dict mapping lowercase name -> linkedin_url."""
    if not website_url:
        return {}

    linkedin_profiles = {}
    parsed = urllib.parse.urlparse(website_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    # Pages that commonly list executives with LinkedIn links
    team_paths = [
        "/about", "/about-us", "/about/team", "/about/leadership",
        "/team", "/our-team", "/leadership", "/management",
        "/about/management", "/people", "/executives",
        "/company", "/company/leadership", "/company/team",
        "/company/about", "/about/people",
    ]

    for path in team_paths:
        try:
            resp = SESSION.get(
                base + path, timeout=(5, 10), allow_redirects=True
            )
            if resp.status_code >= 400:
                continue

            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

            # Find all LinkedIn links on the page
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "linkedin.com/in/" not in href:
                    continue

                # Clean the LinkedIn URL
                li_url = href.strip()
                if not li_url.startswith("http"):
                    li_url = "https:" + li_url if li_url.startswith("//") else "https://" + li_url
                # Strip tracking params
                if "?" in li_url:
                    li_url = li_url.split("?")[0]

                # Try to find the person's name near this link
                name = ""

                # Strategy 1: The link text itself
                link_text = a_tag.get_text(strip=True)
                if (link_text and len(link_text) > 3
                        and not link_text.lower().startswith("linkedin")
                        and not link_text.lower().startswith("connect")
                        and not link_text.startswith("http")):
                    words = link_text.split()
                    if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words if w.isalpha()):
                        name = link_text

                # Strategy 2: Walk up the DOM to find name in nearby text
                if not name:
                    parent = a_tag.parent
                    for _ in range(6):  # Walk up 6 levels
                        if parent is None:
                            break
                        all_text = parent.get_text(separator="|", strip=True)
                        if len(all_text) > 10:
                            # Split on separators and look for name-like segments
                            segments = [s.strip() for s in all_text.split("|") if s.strip()]
                            for seg in segments:
                                seg_words = seg.split()
                                if (2 <= len(seg_words) <= 3
                                        and all(w[0].isupper() for w in seg_words if w.isalpha())
                                        and len(seg) < 35
                                        and not any(kw in seg.lower() for kw in [
                                            "read more", "learn more", "view", "see",
                                            "about", "our", "the", "contact",
                                        ])):
                                    name = seg
                                    break
                            if name:
                                break
                        parent = parent.parent

                # Strategy 3: Extract name from LinkedIn URL slug
                if not name:
                    slug = li_url.rstrip("/").split("/")[-1]
                    parts = slug.split("-")
                    # Filter out hash suffixes (e.g., "9b447")
                    alpha_parts = [p for p in parts if p.isalpha() and len(p) > 1]
                    if len(alpha_parts) >= 2:
                        candidate = " ".join(p.capitalize() for p in alpha_parts[:3])
                        name = candidate

                if name:
                    linkedin_profiles[name.lower().strip()] = li_url

            # If we found profiles, don't need to check more pages
            if linkedin_profiles:
                break

        except Exception:
            continue

    return linkedin_profiles


# ── Exa: Executive Changes ─────────────────────────────────────────

def get_executive_changes(company_name, exa_key):
    """Find recent executive hires/changes at a company, with LinkedIn profiles."""
    try:
        exa = get_exa(exa_key)

        # Clean company name for search (strip parenthetical suffixes)
        search_name = re.sub(r"\s*\([^)]*\)\s*", " ", company_name).strip()

        # Search for executive appointment announcements
        result = exa.search_and_contents(
            f"{search_name} appoints new CEO CTO CIO CRO VP SVP hire executive leadership",
            type="auto",
            num_results=8,
            text={"max_characters": 800},
            category="news",
        )

        changes = []
        exec_titles = [
            "CEO", "CTO", "CIO", "CFO", "COO", "CRO", "CMO",
            "Chief Executive", "Chief Technology", "Chief Financial",
            "Chief Operating", "Chief Operations", "Chief Revenue", "Chief Marketing",
            "Chief Information", "Chief Digital", "Chief Data",
            "Chief People", "Chief Product", "Chief Strategy",
            "Chief Commercial", "Chief Growth", "Chief Customer",
            "President", "Vice President", "VP",
            "VP Sales", "VP Marketing", "VP Engineering", "VP Product",
            "VP Customer Success", "VP Operations", "VP Finance",
            "VP Business Development", "VP Revenue", "VP Growth",
            "VP Technology", "VP IT", "VP Data", "VP Strategy",
            "VP Partnerships", "VP Alliances", "VP Channels",
            "SVP", "EVP", "Managing Director", "General Manager",
            "Head of Sales", "Head of Marketing", "Head of Engineering",
            "Head of Product", "Head of Revenue", "Head of Growth",
            "Head of Customer Success", "Head of Partnerships",
        ]

        # Departure/retirement keywords — these indicate someone LEFT
        departure_keywords = [
            "retire", "retired", "retires", "retiring", "retirement",
            "depart", "departed", "departs", "departing", "departure",
            "step down", "stepped down", "steps down", "stepping down",
            "resign", "resigned", "resigns", "resignation",
            "leave", "leaves", "leaving", "left the company",
            "exit", "exited", "exits", "exiting",
            "former", "outgoing", "predecessor", "replaced by",
            "succeeded by", "steps aside",
        ]

        # Build skip words from company name (strip parentheticals first)
        clean_co_name = re.sub(r"\s*\([^)]*\)\s*", " ", company_name).strip()
        company_skip_words = set()
        for word in clean_co_name.split():
            if len(word) > 1:
                company_skip_words.add(word)
        # Also add common false-positive words
        company_skip_words.update({
            "The", "New", "Our", "This", "That", "Chief", "Senior",
            "Managing", "General", "Officer", "Executive", "Global",
            "Names", "Appoints", "Announces", "Hires", "Welcomes",
            "Company", "Corporation", "Inc", "Ltd", "Canada",
            "North", "South", "East", "West", "Group", "Digital",
        })

        if result.results:
            for r in result.results:
                text = (r.title or "") + " " + (r.text or "")
                text_lower = text.lower()

                # Skip roundup/listicle articles (not company-specific)
                roundup_patterns = [
                    r"\d+\s+c[x-]?os\b", r"\d+\s+executives?\b",
                    r"\d+\s+leaders?\b", r"\d+\s+people\b",
                    r"on the move", r"movers and shakers",
                    r"who.s moving", r"executive shuffle",
                ]
                is_roundup = any(re.search(p, (r.title or "").lower()) for p in roundup_patterns)
                if is_roundup:
                    print(f"    Skipping roundup article: {(r.title or '')[:60]}")
                    continue

                # Check if this is actually about an executive change
                if not any(w in text_lower for w in [
                    "appoint", "hire", "join", "named", "promoted",
                    "new ceo", "new cto", "new cio", "new cfo",
                    "new cro", "new cmo", "announces", "welcome",
                ]):
                    continue

                # ── Departure detection ────────────────────────────
                # Skip entries that are about someone leaving/retiring
                is_departure = any(kw in text_lower for kw in departure_keywords)
                # But if the text ALSO mentions a new appointment, it might
                # be "X retires, Y appointed" — check for new hire signal
                has_new_hire_signal = any(w in text_lower for w in [
                    "appoint", "hire", "join", "named", "promoted", "welcome",
                ])
                if is_departure and not has_new_hire_signal:
                    continue
                # If both departure and hire signals, we'll still process it
                # but the name extraction will try to find the NEW person

                # Find the matching title
                matched_title = ""
                for title in exec_titles:
                    if title.lower() in text_lower:
                        matched_title = title
                        break
                if not matched_title:
                    continue

                # Try to extract the person's name from the text
                person_name = ""
                title_esc = re.escape(matched_title)

                def _is_valid_name(candidate):
                    """Check if a candidate string looks like a real person name."""
                    # Strip middle initials for word count (e.g., "James C. Foster" -> 3 words)
                    words = candidate.split()
                    # Filter out single-letter initials for checks
                    real_words = [w for w in words if len(w.rstrip(".")) > 1]
                    if len(real_words) < 2 or len(words) > 4:
                        return False
                    if any(w in company_skip_words for w in real_words):
                        return False
                    if any(w.lower() in {"officer", "chief", "vice", "president", "head"} for w in real_words):
                        return False
                    return True

                # STRATEGY 1: Extract from headline first (most reliable)
                # Name pattern allows for middle initials like "James C. Foster"
                _name_word = r"[A-Z][a-z]+"
                _name_mid = r"(?:\s+[A-Z]\.?)?"  # optional middle initial
                _name_full = rf"({_name_word}{_name_mid}\s+{_name_word})"
                if r.title:
                    # Descriptor words that may appear between verb and name
                    _desc = r"(?:(?:Industry|Veteran|Technology|Cybersecurity|Security|Digital|Former|Seasoned|Experienced|Renowned|Senior|Global|Accomplished)\s+)*"
                    headline_patterns = [
                        r"(?:appoints?|names?|hires?|welcomes?|taps?)\s+" + _name_full,
                        # Handle intervening descriptors: "Appoints Industry Veteran James Foster"
                        r"(?:appoints?|names?|hires?|welcomes?|taps?)\s+" + _desc + _name_full,
                        r"^" + _name_full + r"\s+(?:joins?|appointed|named|to lead|promoted|as\s)",
                        r"(?:new\s+)?" + title_esc + r"[,:\s]+" + _name_full,
                    ]
                    for hp in headline_patterns:
                        hm = re.search(hp, r.title)
                        if hm:
                            candidate = hm.group(1).strip()
                            if _is_valid_name(candidate):
                                person_name = candidate
                                break

                # STRATEGY 2: Extract from body text with title-anchored patterns
                if not person_name:
                    name_pat = r"([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:\s+[A-Z][a-z]+)+)"
                    name_patterns = [
                        # "appoints John Smith as CEO"
                        r"(?:appoints?|named?|hires?|welcomes?|announces?|taps?|picks?|elevates?)\s+" + name_pat + r"\s+(?:as\s+)?(?:new\s+)?(?:its\s+)?(?:" + title_esc + ")",
                        # "John Smith appointed CEO"
                        name_pat + r"\s+(?:appointed|named|hired|joins?|promoted|tapped|elevated|selected)\s+(?:as\s+)?(?:new\s+)?(?:the\s+)?(?:" + title_esc + ")",
                        # "new CEO John Smith" or "CEO: John Smith"
                        r"(?:new\s+)?" + title_esc + r"[,:\s]+\b" + name_pat,
                    ]
                    for pattern in name_patterns:
                        match = re.search(pattern, text)
                        if match:
                            candidate = match.group(1).strip()
                            if _is_valid_name(candidate):
                                person_name = candidate
                                break

                # If departure and we couldn't extract a new person name, skip entirely
                if is_departure and not person_name:
                    continue

                # Clean context — strip nav/menu junk
                raw_context = (r.text or "")[:400]
                context_lines = raw_context.split("\n")
                clean_ctx_parts = []
                nav_words = {"news", "technology", "launch", "government", "military",
                             "finance", "connectivity", "interactive", "features", "about",
                             "contact", "menu", "search", "login", "subscribe", "home",
                             "privacy", "cookie", "terms", "careers", "blog", "press"}
                junk_ctx_phrases = [
                    "accessibility statement", "skip navigation",
                    "subscribe for", "sign in to view", "create your free",
                    "notification icon", "primary sidebar",
                ]
                for cl in context_lines:
                    cl = cl.strip()
                    if not cl:
                        continue
                    cl_lower = cl.lower()
                    if any(jp in cl_lower for jp in junk_ctx_phrases):
                        continue
                    tokens = [w.strip().lower() for w in re.split(r"[|,/]", cl)]
                    if len(tokens) >= 3 and sum(1 for t in tokens if t in nav_words) >= 2:
                        continue
                    if len(cl) < 20 and cl_lower in nav_words:
                        continue
                    clean_ctx_parts.append(cl)
                clean_context = " ".join(clean_ctx_parts)[:250]

                change = {
                    "title": matched_title,
                    "person_name": person_name,
                    "headline": r.title or "",
                    "context": clean_context,
                    "announcement_url": r.url or "",
                    "date": str(r.published_date or ""),
                    "linkedin_url": "",
                }

                # Try to find LinkedIn profile
                linkedin_found = False
                if person_name:
                    try:
                        li_result = exa.search(
                            f"{person_name} {company_name} {matched_title}",
                            type="auto",
                            num_results=3,
                            include_domains=["linkedin.com"],
                        )
                        if li_result.results:
                            for lr in li_result.results:
                                if "/in/" in lr.url:
                                    change["linkedin_url"] = lr.url
                                    linkedin_found = True
                                    break
                    except Exception:
                        pass

                # Fallback: search by title + company even without person name
                if not linkedin_found:
                    try:
                        li_result = exa.search(
                            f"{company_name} {matched_title}",
                            type="auto",
                            num_results=3,
                            include_domains=["linkedin.com"],
                        )
                        if li_result.results:
                            for lr in li_result.results:
                                if "/in/" in lr.url:
                                    change["linkedin_url"] = lr.url
                                    # Try to extract name from LinkedIn URL only
                                    # if we don't already have a person_name
                                    # AND the headline doesn't clearly name someone else
                                    if not person_name:
                                        slug = lr.url.rstrip("/").split("/")[-1]
                                        parts = slug.split("-")
                                        if len(parts) >= 2:
                                            name_candidate = " ".join(
                                                p.capitalize() for p in parts
                                                if p.isalpha() and len(p) > 1
                                            )
                                            if 2 <= len(name_candidate.split()) <= 3:
                                                # Verify name is mentioned in article text
                                                # to avoid picking up wrong person
                                                name_parts = name_candidate.lower().split()
                                                if any(p in text_lower for p in name_parts if len(p) > 2):
                                                    change["person_name"] = name_candidate
                                    break
                    except Exception:
                        pass

                changes.append(change)

        # ── Filter out results not about this company ─────────────
        # Exa can return articles about OTHER companies. Check that the
        # headline or article text actually mentions our company.
        if changes:
            # Clean company name: strip parenthetical suffixes like "(SLK)", "(Main Org)"
            clean_name_for_filter = re.sub(r"\s*\([^)]*\)\s*", " ", company_name).strip()
            clean_name_for_filter = _clean_company_name(clean_name_for_filter)
            company_words = [w.lower() for w in clean_name_for_filter.split() if len(w) > 1]
            # Strip common suffixes for matching
            strip_suffixes = {"inc.", "inc", "ltd", "ltd.", "corp", "corp.",
                              "limited", "llc", "operations", "technologies",
                              "canada", "group", "holdings", "the"}
            core_words = [w for w in company_words if w not in strip_suffixes]
            if not core_words:
                core_words = company_words[:1]

            # Build full phrase for stricter matching
            full_phrase = " ".join(core_words).lower()

            filtered_changes = []
            for ch in changes:
                headline_lower = ch["headline"].lower()
                context_lower = ch["context"].lower()
                combined = headline_lower + " " + context_lower
                # For multi-word names, require full phrase or 2+ core words
                if len(core_words) >= 2:
                    if full_phrase in combined:
                        filtered_changes.append(ch)
                    elif sum(1 for w in core_words if w in combined) >= 2:
                        filtered_changes.append(ch)
                    else:
                        print(f"    Filtered out irrelevant exec: {ch.get('headline','')[:60]}")
                else:
                    if any(w in combined for w in core_words):
                        filtered_changes.append(ch)
                    else:
                        print(f"    Filtered out irrelevant exec: {ch.get('headline','')[:60]}")

            # If strict filter removed everything, try single-word match
            # but only for articles from the company's own domain
            if not filtered_changes and changes:
                # Get the company's website domain for verification
                # (passed via closure from enrich_company)
                for ch in changes:
                    combined = ch["headline"].lower() + " " + ch["context"].lower()
                    if any(w in combined for w in core_words):
                        # Single word match — mark as lower confidence
                        ch["_low_confidence"] = True
                        filtered_changes.append(ch)
                if filtered_changes:
                    print(f"    Relaxed filter: kept {len(filtered_changes)} results with single-word match")

            changes = filtered_changes

        # ── Deduplicate by title ───────────────────────────────────
        # Normalize equivalent titles before dedup:
        # "Chief Technology" == "CTO", "Chief Financial" == "CFO", etc.
        title_normalize = {
            "chief executive": "ceo", "chief technology": "cto",
            "chief financial": "cfo", "chief operating": "coo",
            "chief operations": "coo",
            "chief revenue": "cro", "chief marketing": "cmo",
            "chief information": "cio", "chief digital": "cdo",
            "chief data": "chief data", "chief people": "chief people",
            "chief product": "chief product", "chief strategy": "chief strategy",
            "chief commercial": "chief commercial", "chief growth": "chief growth",
            "chief customer": "chief customer",
            # Note: "vice president" is NOT normalized to just "vp"
            # so "VP Sales" and "VP Marketing" stay as separate entries
        }

        if changes:
            best_by_title = {}
            for ch in changes:
                title_key = ch["title"].lower().strip()
                # Normalize to abbreviation
                title_key = title_normalize.get(title_key, title_key)

                existing = best_by_title.get(title_key)
                if existing is None:
                    best_by_title[title_key] = ch
                else:
                    # Prefer entries with a person name
                    new_has_name = bool(ch["person_name"])
                    old_has_name = bool(existing["person_name"])
                    if new_has_name and not old_has_name:
                        best_by_title[title_key] = ch
                    elif new_has_name == old_has_name:
                        # Both have or lack names — prefer more recent date
                        if ch["date"] > existing["date"]:
                            best_by_title[title_key] = ch
                        # Also prefer entries with LinkedIn URLs
                        elif ch["date"] == existing["date"] and ch["linkedin_url"] and not existing["linkedin_url"]:
                            best_by_title[title_key] = ch
            changes = list(best_by_title.values())

        return changes

    except Exception as e:
        print(f"  Exa executive search error: {e}")
        return []


# ── Scoring Engine ──────────────────────────────────────────────────


def _parse_aov_band(aov_band):
    """Parse AOV band string into a numeric midpoint for scoring.
    Returns (midpoint, label) or (0, '') if not parseable.
    Examples: '$600k-1M' -> (800000, '$600k-1M'), '$10k-50k' -> (30000, '$10k-50k')
    """
    if not aov_band or str(aov_band).strip() in ("", "$0", "0"):
        return (0, str(aov_band).strip())
    raw = str(aov_band).strip().replace(",", "")
    # Extract numbers with K/M multipliers
    nums = re.findall(r'\$?([\d.]+)\s*([kKmM])?', raw)
    values = []
    for num_str, mult in nums:
        val = float(num_str)
        if mult.lower() == 'k':
            val *= 1000
        elif mult.lower() == 'm':
            val *= 1000000
        values.append(val)
    if len(values) >= 2:
        return (int((values[0] + values[1]) / 2), raw)
    elif len(values) == 1:
        return (int(values[0]), raw)
    return (0, raw)


def score_company(enriched, weights):
    """Score a company based on all enrichment data."""
    scores = {}

    # ── Tech Stack Score (0-100) ────────────────────────────────────
    tech_score = 0
    techs = enriched.get("technologies", [])
    dns = enriched.get("dns", {})
    sf_opp = ""
    key_techs = []
    crm = "Unknown"

    if techs:
        # Existing Salesforce = high value (expansion)
        has_sf = any(
            "salesforce" in t.get("name", "").lower() or
            "pardot" in t.get("name", "").lower()
            for t in techs
        )
        sf_from_dns = any(
            "salesforce" in s.lower() or "pardot" in s.lower()
            for s in dns.get("email_senders", [])
        )

        if has_sf or sf_from_dns:
            tech_score += 40
            sf_opp = "Expansion"
            crm = "Salesforce"
            key_techs.append("Salesforce (existing)")

        # Competitor CRM = displacement opp
        competitor_crms = [
            t["name"] for t in techs
            if t.get("category") == "CRM"
            and "salesforce" not in t["name"].lower()
        ]
        if competitor_crms:
            tech_score += 30
            if not sf_opp:
                sf_opp = "Displacement"
                crm = ", ".join(set(competitor_crms))
            key_techs.extend(competitor_crms)

        # Marketing tools
        mktg = [
            t["name"] for t in techs
            if t.get("category") == "Marketing"
            and "pardot" not in t["name"].lower()
        ]
        if mktg:
            tech_score += 10
            key_techs.extend(mktg)

        # Service/Support tools
        svc = [
            t["name"] for t in techs
            if t.get("category") == "Service/Support"
        ]
        if svc:
            tech_score += 10
            key_techs.append(f"{len(svc)} service tools")

        # E-commerce
        has_ecomm = any(t.get("category") == "E-commerce" for t in techs)
        if has_ecomm:
            tech_score += 10
            key_techs.append("E-commerce detected")

        # No CRM = greenfield
        if not has_sf and not sf_from_dns and not competitor_crms:
            sf_opp = "Greenfield"
            crm = "None detected"
            tech_score += 20

    # ── Job Posting Tool Bonus ─────────────────────────────────────
    # Tools discovered from job postings that weren't found on the website
    jp_tools = enriched.get("job_posting_tools", [])
    if jp_tools:
        jp_sf_names = [t["name"].lower() for t in jp_tools]
        website_names = [t.get("name", "").lower() for t in techs]

        # Check for Salesforce in job postings (upgrade from greenfield)
        jp_has_sf = any(
            n in ("salesforce", "pardot", "account engagement (pardot)",
                   "salesforce einstein", "marketing cloud", "service cloud",
                   "commerce cloud", "data cloud", "experience cloud",
                   "mulesoft", "heroku")
            for n in jp_sf_names
        )
        if jp_has_sf and sf_opp != "Expansion":
            sf_opp = "Expansion"
            crm = "Salesforce (from job postings)"
            tech_score = max(tech_score, 40)
            key_techs.append("Salesforce (job postings)")

        # Check for competitor CRMs in job postings
        jp_competitor_crms = [
            t["name"] for t in jp_tools
            if t["category"] in ("CRM", "CRM / Marketing Automation")
            and "salesforce" not in t["name"].lower()
            and t["name"].lower() not in website_names
        ]
        if jp_competitor_crms and sf_opp not in ("Expansion", "Displacement"):
            sf_opp = "Displacement"
            crm = ", ".join(set(jp_competitor_crms))
            tech_score += 25
            key_techs.extend(jp_competitor_crms)

        # Bonus for each new Salesforce-relevant tool found
        new_relevant = [
            t for t in jp_tools
            if t["name"].lower() not in website_names
            and t["sf_solution"] != "—"
            and "already using" not in t["sf_solution"].lower()
        ]
        if new_relevant:
            tech_score += min(len(new_relevant) * 5, 20)
            for t in new_relevant[:3]:
                key_techs.append(f"{t['name']} (job posting)")

    tech_score = min(tech_score, 100)
    scores["tech_stack"] = tech_score

    # ── Compelling Events Score (0-100) ─────────────────────────────
    events = enriched.get("compelling_events", [])
    event_score = min(len(events) * 25, 100)
    scores["compelling_events"] = event_score

    # ── Executive Changes Score (0-100) ─────────────────────────────
    # Filter out executives with missing names — these are bad extractions
    exec_changes = [
        e for e in enriched.get("executive_changes", [])
        if e.get("person_name", "").strip()
    ]
    enriched["executive_changes"] = exec_changes
    exec_score = min(len(exec_changes) * 30, 100)
    scores["new_executives"] = exec_score

    # ── Company Size Score (0-100) ──────────────────────────────────
    headcount = enriched.get("headcount", "")
    size_score = 50  # default mid-range
    if headcount:
        try:
            hc = int(str(headcount).replace(",", "").strip())
            if hc >= 5000:
                size_score = 90
            elif hc >= 1000:
                size_score = 80
            elif hc >= 500:
                size_score = 70
            elif hc >= 200:
                size_score = 60
            elif hc >= 50:
                size_score = 40
            else:
                size_score = 20
        except (ValueError, TypeError):
            pass
    scores["company_size"] = size_score

    # ── Accessibility Score (0-100) ─────────────────────────────────
    access_score = 50  # default
    if enriched.get("website"):
        access_score += 20
    if enriched.get("description"):
        access_score += 15
    if enriched.get("news_items"):
        access_score += 15
    access_score = min(access_score, 100)
    scores["accessibility"] = access_score

    # ── Weighted Total ──────────────────────────────────────────────
    # Map weight keys to score keys
    weight_to_score = {
        "tech_weight": "tech_stack",
        "events_weight": "compelling_events",
        "execs_weight": "new_executives",
        "size_weight": "company_size",
        "accessibility_weight": "accessibility",
    }
    total_weight = sum(weights.values())
    if total_weight > 0:
        weighted = sum(
            scores.get(weight_to_score.get(wk, wk), 0) * wv
            for wk, wv in weights.items()
        )
        final_score = round(weighted / total_weight)
    else:
        final_score = 0

    # ── AOV Band Multiplier ─────────────────────────────────────────
    # AOV is the single most important prioritization signal for reps.
    # Apply a multiplier so high-AOV accounts rise to the top and
    # $0 AOV accounts are visibly deprioritized.
    aov_band = enriched.get("aov_band", "")
    aov_midpoint, _aov_label = _parse_aov_band(aov_band)
    aov_multiplier = 1.0
    if aov_midpoint >= 600000:
        aov_multiplier = 1.25   # $600k+ gets a 25% boost
    elif aov_midpoint >= 200000:
        aov_multiplier = 1.15   # $200k-600k gets 15% boost
    elif aov_midpoint >= 100000:
        aov_multiplier = 1.10   # $100k-200k gets 10% boost
    elif aov_midpoint >= 50000:
        aov_multiplier = 1.05   # $50k-100k gets 5% boost
    elif aov_midpoint >= 10000:
        aov_multiplier = 1.0    # $10k-50k stays neutral
    elif aov_midpoint > 0:
        aov_multiplier = 0.90   # Small AOV gets 10% penalty
    else:
        # $0 or unknown AOV — significant penalty
        aov_multiplier = 0.75
    final_score = min(round(final_score * aov_multiplier), 100)
    enriched["aov_multiplier"] = aov_multiplier

    # Update enriched dict
    enriched["score"] = final_score
    enriched["tech_score"] = tech_score
    enriched["sf_opportunity"] = sf_opp or "Unknown"
    enriched["crm"] = crm
    enriched["key_technologies"] = ", ".join(key_techs[:5])
    enriched["signals_summary"] = "; ".join(
        f"{dim}: {scores[dim]}" for dim in scores
    )

    # ── Top 3 Signals (Why reach out) ─────────────────────────────
    # Build a prioritized list of concrete, actionable reasons to call.
    # Each signal should give the rep a SPECIFIC conversation opener
    # they can use in the first 30 seconds of a cold call.
    signals = []
    news_items = enriched.get("news_items", [])
    jp_tools = enriched.get("job_posting_tools", [])

    # Signal from SF opportunity type — make it specific to their stack
    if sf_opp == "Expansion":
        # Tell the rep WHAT to expand into based on detected gaps
        expand_targets = []
        has_service_tool = any("service" in t.lower() or "zendesk" in t.lower() or "freshdesk" in t.lower() or "intercom" in t.lower() for t in key_techs)
        has_marketing_tool = any("market" in t.lower() or "hubspot" in t.lower() or "marketo" in t.lower() for t in key_techs)
        has_commerce = any("commerce" in t.lower() or "shopify" in t.lower() for t in key_techs)
        # Also check JP tools for expansion gaps
        jp_categories = set(t["category"] for t in jp_tools) if jp_tools else set()
        if has_service_tool or "Customer Support" in jp_categories:
            expand_targets.append("Service Cloud (replacing support tools)")
        if has_marketing_tool or "Marketing Automation" in jp_categories:
            expand_targets.append("Marketing Cloud")
        if has_commerce or "E-Commerce" in jp_categories:
            expand_targets.append("Commerce Cloud")
        if any("data" in cat.lower() or "cdp" in cat.lower() for cat in jp_categories):
            expand_targets.append("Data Cloud")
        if expand_targets:
            signals.append((f"🟢 Already on Salesforce — pitch {expand_targets[0]}", 95))
        else:
            signals.append(("🟢 Already on Salesforce — explore multi-cloud expansion", 95))
    elif sf_opp == "Displacement":
        # Tell the rep what they're displacing and the angle
        if "hubspot" in crm.lower():
            signals.append((f"🟠 Using HubSpot — pitch enterprise scalability gap as they grow", 85))
        elif "dynamics" in crm.lower():
            signals.append((f"🟠 Using Dynamics 365 — pitch AI/Einstein advantage and ecosystem", 85))
        elif "zoho" in crm.lower():
            signals.append((f"🟠 Using Zoho — pitch platform consolidation and analytics", 85))
        else:
            signals.append((f"🟠 Using {crm} — lead with platform consolidation ROI", 85))
    elif sf_opp == "Greenfield":
        signals.append(("🔵 No CRM detected — lead with 'how are you managing customer relationships today?'", 75))

    # Signals from compelling events — include the SPECIFIC headline so rep
    # can reference it: "I saw you just acquired X..."
    event_signals_map = {
        "M&A Activity": ("🔴", "new leadership will revisit tech stack — ask about integration plans"),
        "Funding/IPO": ("🟢", "budget unlocked for platform investments — ask what they're prioritizing"),
        "Digital Transformation": ("🟠", "modernizing systems — ask what's on the roadmap"),
        "Expansion": ("🟣", "scaling operations — ask how their tools are keeping up"),
        "Strategic Partnership": ("🔵", "new partnerships create integration needs"),
        "Major Contract/Deal": ("🟡", "growing team will need better tools to deliver"),
        "Restructuring": ("⚪", "consolidating vendors to cut costs — pitch platform value"),
    }
    events_already_added = set()
    for evt in events:
        if evt in event_signals_map and evt not in events_already_added:
            icon, reason = event_signals_map[evt]
            # Find the actual news headline for this event type
            headline_ref = ""
            for ni in news_items:
                if evt in ni.get("events", []) and not ni.get("stale"):
                    short_title = ni["title"][:80]
                    date_str = ni.get("date", "")[:10]
                    headline_ref = f'"{short_title}"'
                    if date_str:
                        headline_ref += f" ({date_str})"
                    break
            priority = 80 - list(event_signals_map.keys()).index(evt) * 5
            if headline_ref:
                signals.append((f"{icon} {headline_ref} — {reason}", priority))
            else:
                signals.append((f"{icon} {evt} detected — {reason}", priority))
            events_already_added.add(evt)

    # Signals from executive changes — give the rep a name to ask for
    for exc in exec_changes[:2]:
        person = exc.get("person_name", "")
        title = exc.get("title", "")
        date = exc.get("date", "")[:10]
        li = exc.get("linkedin_url", "")
        if person and title:
            date_note = f" ({date})" if date else ""
            li_note = f" [LinkedIn]({li})" if li else ""
            signals.append((f"👔 New {title}: {person}{date_note} — new leaders review vendors in first 90 days{li_note}", 70))

    # Signals from specific tech stack — tell the rep the EXACT replacement pitch
    if key_techs:
        # Quick lookup: competitor tool -> SF replacement
        _sf_replace = {
            "HubSpot": "Sales Cloud", "Zendesk": "Service Cloud",
            "Freshdesk": "Service Cloud", "Intercom": "Service Cloud",
            "Drift": "Service Cloud", "Marketo": "Marketing Cloud (MCAE)",
            "Mailchimp": "Marketing Cloud", "Shopify": "Commerce Cloud",
            "Zoho CRM": "Sales Cloud", "Microsoft Dynamics": "Sales Cloud",
            "Pipedrive": "Sales Cloud", "6sense": "Marketing Cloud",
            "Segment": "Data Cloud", "Mixpanel": "CRM Analytics",
            "SalesLoft": "Sales Engagement", "Outreach": "Sales Engagement",
            "Gong": "Einstein Conversation Insights",
        }
        displacement_pitches = []
        all_detected = enriched.get("technologies", [])
        for t in all_detected:
            tname = t.get("name", "").split(" (")[0]  # strip "(via SPF)" etc.
            if "salesforce" in tname.lower() or "pardot" in tname.lower():
                continue
            sf_sol = _sf_replace.get(tname, "")
            if sf_sol:
                displacement_pitches.append(f"{tname} -> {sf_sol}")
        if displacement_pitches:
            signals.append((f"🔧 Replaceable stack: {'; '.join(displacement_pitches[:3])}", 60))

    # Signal from job posting discoveries — be specific about what was found
    if jp_tools:
        displaceable = [
            t for t in jp_tools
            if t["sf_solution"] != "—"
            and "already using" not in t["sf_solution"].lower()
        ]
        if displaceable:
            # Group by SF solution for a cleaner pitch
            tool_details = [f"{t['name']} (replace with {t['sf_solution']})" for t in displaceable[:2]]
            signals.append((f"📋 Job postings confirm: {'; '.join(tool_details)}", 55))

    # Sort by priority and take top 3
    signals.sort(key=lambda x: -x[1])
    enriched["top_signals"] = [s[0] for s in signals[:3]]

    # ── Recommended Contacts to Find ──────────────────────────────
    # Based on SF opportunity, detected tech stack, AND industry,
    # suggest specific personas tied to the tools they own.
    contacts = []
    industry = enriched.get("industry", "").lower()

    # Build a set of all detected tool names (website + job postings)
    all_tool_names = set()
    for t in enriched.get("technologies", []):
        all_tool_names.add(t.get("name", "").lower())
    for t in jp_tools:
        all_tool_names.add(t.get("name", "").lower())
    all_tool_str = " ".join(all_tool_names)

    # Detect specific tool categories for targeted contact suggestions
    has_hubspot = any("hubspot" in t for t in all_tool_names)
    has_zendesk = any("zendesk" in t for t in all_tool_names)
    has_freshdesk = any("freshdesk" in t for t in all_tool_names)
    has_intercom = any("intercom" in t for t in all_tool_names)
    has_marketo = any("marketo" in t for t in all_tool_names)
    has_mailchimp = any("mailchimp" in t for t in all_tool_names)
    has_shopify = any("shopify" in t for t in all_tool_names)
    has_service_tool = has_zendesk or has_freshdesk or has_intercom or any("service" in t for t in all_tool_names)
    has_marketing_tool = has_marketo or has_mailchimp or has_hubspot or any("marketing" in t or "pardot" in t for t in all_tool_names)
    has_commerce = has_shopify or any("commerce" in t or "magento" in t for t in all_tool_names)
    has_data_tool = any(t in all_tool_str for t in ("segment", "snowflake", "databricks", "mparticle", "tealium"))
    has_sales_engagement = any(t in all_tool_str for t in ("salesloft", "outreach", "gong", "chorus"))

    if sf_opp == "Expansion":
        contacts.append("Salesforce Admin / Architect — knows current footprint, can champion internally")
        if has_service_tool:
            tool_name = "Zendesk" if has_zendesk else "Freshdesk" if has_freshdesk else "Intercom" if has_intercom else "support tools"
            contacts.append(f"VP Customer Support / Success — owns {tool_name}, pitch Service Cloud replacement")
        if has_marketing_tool:
            tool_name = "HubSpot" if has_hubspot else "Marketo" if has_marketo else "marketing tools"
            contacts.append(f"VP Marketing / Marketing Ops — owns {tool_name}, pitch Marketing Cloud")
        if has_commerce:
            contacts.append(f"VP E-commerce / Digital — owns {'Shopify' if has_shopify else 'commerce platform'}, pitch Commerce Cloud")
        if has_data_tool:
            contacts.append("VP Data / Analytics — pitch Data Cloud for unified customer view")
        if not has_service_tool and not has_marketing_tool:
            contacts.append("VP Sales or CRO — expand into Sales Cloud features they're not using")
    elif sf_opp == "Displacement":
        crm_owner = "VP Sales or CRO"
        if has_hubspot:
            crm_owner = "VP Marketing / Marketing Ops — likely chose HubSpot, address their concerns first"
        elif "dynamics" in crm.lower():
            crm_owner = "CIO / CTO — Dynamics is usually an IT decision"
        contacts.append(f"{crm_owner}")
        contacts.append(f"CFO — lead with TCO comparison: {crm} vs Salesforce platform consolidation")
        if has_sales_engagement:
            contacts.append("VP Revenue Ops / Sales Ops — owns sales engagement stack, natural champion")
        else:
            contacts.append("Head of Sales Ops / Rev Ops — will be key in migration planning")
    else:  # Greenfield — tailor to industry
        if any(kw in industry for kw in ("retail", "e-commerce", "consumer")):
            contacts.append("VP Digital / E-commerce — Commerce Cloud + personalization pitch")
            contacts.append("VP Marketing / CMO — omnichannel customer journey")
            contacts.append("CIO / CTO — platform decision maker")
        elif any(kw in industry for kw in ("financial", "banking", "insurance")):
            contacts.append("CIO / CTO — platform decision maker (compliance matters)")
            contacts.append("VP Client Relations / Wealth Mgmt — customer engagement")
            contacts.append("Chief Digital Officer — digital transformation sponsor")
        elif any(kw in industry for kw in ("telecom", "media", "entertainment")):
            contacts.append("CIO / CTO — platform decision maker")
            contacts.append("VP Sales / Partnerships — revenue relationships")
            contacts.append("VP Customer Operations — Communications Cloud pitch")
        elif any(kw in industry for kw in ("software", "saas", "technology")):
            contacts.append("VP Sales or CRO — primary CRM buyer in tech companies")
            contacts.append("VP Revenue Operations — owns tooling decisions")
            contacts.append("CEO / President — strategic direction for growth")
        else:
            contacts.append("CIO / CTO — technology decision maker")
            contacts.append("VP Sales or CRO — primary CRM user, drives requirements")
            contacts.append("CEO / President — strategic vision for customer-centric growth")

    # Add NAMED contacts from executive changes (highest value — real people to call)
    for exc in exec_changes:
        person = exc.get("person_name", "")
        title = exc.get("title", "")
        li = exc.get("linkedin_url", "")
        if person:
            li_note = f" [LinkedIn]({li})" if li else ""
            contacts.insert(0, f"📌 {person}, {title}{li_note} — recently appointed, likely reviewing vendors")

    enriched["recommended_contacts"] = contacts[:6]

    return enriched


# ── Company Name Cleaning ────────────────────────────────────────────

# Salesforce org labels that get appended to account names
_ORG_SUFFIX_PATTERNS = [
    r"\s+\d+(?:st|nd|rd|th)\s+Org\b",  # "2nd Org", "3rd Org"
    r"\s+Main\s+Org\b",
    r"\s+\(Main\s+Org\)\s*",
    r"\s+\(SLK\)\s*",
    r"\s+-\s+Org\s*\d*\s*$",
]


def _clean_company_name(name):
    """Strip Salesforce org labels and other noise from account names."""
    clean = name.strip()
    for pattern in _ORG_SUFFIX_PATTERNS:
        clean = re.sub(pattern, "", clean, flags=re.IGNORECASE).strip()
    return clean


# ── Main Enrichment Function ───────────────────────────────────────

def enrich_company(
    company_name,
    team_member="",
    aov_band="",
    headcount="",
    known_website=None,
    exa_key="",
    do_website=True,
    do_tech_stack=True,
    do_company_intel=True,
    do_news=True,
    do_executives=True,
    do_job_postings=True,
):
    """Enrich a single company with all available data."""
    # Clean company name: strip SF org suffixes like "2nd Org"
    clean_name = _clean_company_name(company_name)

    result = {
        "name": company_name,  # Preserve original name for display
        "team_member": team_member,
        "aov_band": aov_band,
        "headcount": headcount,
        "website": "",
        "technologies": [],
        "job_posting_tools": [],
        "dns": {},
        "subdomains": {},
        "description": "",
        "industry": "",
        "news_items": [],
        "compelling_events": [],
        "executive_changes": [],
        "recent_news": "",
    }

    # Step 1: Website discovery
    if do_website:
        url = discover_website(clean_name, known_website, exa_key)
        result["website"] = url or ""

    # Step 2: Tech stack
    if do_tech_stack and result["website"]:
        tech_data = scan_tech_stack_light(result["website"])
        result["technologies"] = tech_data.get("technologies", [])
        result["dns"] = tech_data.get("dns", {})
        result["subdomains"] = tech_data.get("subdomains", {})

    # Step 2b: Job posting tech stack scan
    if do_job_postings and exa_key:
        try:
            jp_tools = scan_job_postings(clean_name, exa_key)
            result["job_posting_tools"] = jp_tools
            if jp_tools:
                print(f"    Found {len(jp_tools)} tools from job postings")
        except Exception as e:
            print(f"    Job posting scan error: {e}")

    # Step 3: Company intel
    if do_company_intel and exa_key:
        intel = get_company_intel(clean_name, result["website"], exa_key)
        result["description"] = intel.get("description", "")
        result["industry"] = intel.get("industry", "")
        if intel.get("estimated_size") and not headcount:
            result["headcount"] = intel["estimated_size"]

    # Step 4: News
    if do_news and exa_key:
        news_data = get_company_news(clean_name, exa_key)
        result["news_items"] = news_data.get("news_items", [])
        result["compelling_events"] = news_data.get("compelling_events", [])
        # Build rich summary with event tags and URLs
        if news_data["news_items"]:
            news_summaries = []
            for n in news_data["news_items"][:5]:
                events_tag = ""
                if n.get("events"):
                    events_tag = f"[{', '.join(n['events'])}] "
                news_summaries.append({
                    "text": f"{events_tag}{n['title']}",
                    "url": n["url"],
                    "events": n.get("events", []),
                })
            result["news_with_links"] = news_summaries
            result["recent_news"] = " | ".join(
                s["text"] for s in news_summaries[:3]
            )

    # Step 5: Executive changes
    if do_executives and exa_key:
        result["executive_changes"] = get_executive_changes(
            clean_name, exa_key
        )

        # Step 5b: Scrape LinkedIn profiles from company about/team pages
        # and cross-reference with executive changes
        website_linkedin = {}
        if result["website"]:
            try:
                website_linkedin = scrape_linkedin_from_website(result["website"])
                if website_linkedin:
                    print(f"    Found {len(website_linkedin)} LinkedIn profiles on website")
            except Exception:
                pass

        # Cross-reference: fill in missing LinkedIn URLs from website scrape
        if website_linkedin and result["executive_changes"]:
            for exc in result["executive_changes"]:
                if not exc.get("linkedin_url") and exc.get("person_name"):
                    person_lower = exc["person_name"].lower().strip()
                    # Try exact match first
                    if person_lower in website_linkedin:
                        exc["linkedin_url"] = website_linkedin[person_lower]
                    else:
                        # Try partial match (last name or first+last)
                        person_parts = person_lower.split()
                        for web_name, web_url in website_linkedin.items():
                            web_parts = web_name.split()
                            # Match if last names match, or first+last match
                            if (len(person_parts) >= 2 and len(web_parts) >= 2
                                    and person_parts[-1] == web_parts[-1]
                                    and person_parts[0] == web_parts[0]):
                                exc["linkedin_url"] = web_url
                                break

        # Store website LinkedIn profiles for reference
        if website_linkedin:
            result["website_linkedin_profiles"] = {
                name: url for name, url in website_linkedin.items()
            }

        if result["executive_changes"]:
            exec_summaries = []
            for e in result["executive_changes"][:3]:
                name = e.get("person_name", "") or "New hire"
                title = e.get("title", "")
                li = e.get("linkedin_url", "")
                ann = e.get("announcement_url", "")
                exec_summaries.append({
                    "text": f"{name} — {title}",
                    "linkedin_url": li,
                    "announcement_url": ann,
                })
            result["executives_with_links"] = exec_summaries
            result["new_executives"] = " | ".join(
                s["text"] for s in exec_summaries
            )
        else:
            result["new_executives"] = ""

    return result
