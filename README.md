# MondayMorning Platform
## Automated Competitive Intelligence for Homebuilders

[![Prototype Status](https://img.shields.io/badge/Status-Demo--Ready%20Prototype-orange)](README.md)
[![Industry](https://img.shields.io/badge/Industry-Homebuilding-blue)](README.md)
[![Market](https://img.shields.io/badge/Market-Calgary%20AB-green)](README.md)

---

## 🎯 Executive Summary

MondayMorning is an **automated, AI-driven competitive intelligence platform** specifically designed for homebuilders. It transforms manual, time-consuming competitive analysis into automated insights that drive faster, data-driven business decisions.

### The Problem
- **Reactive Decision Making**: Companies adjust pricing after opportunities are lost
- **Manual Intelligence**: Sporadic, inconsistent competitive data collection
- **No Benchmarking**: Limited visibility into market positioning vs. true competitors
- **Time-Intensive**: Hours spent on manual research that becomes outdated quickly

### The Solution
- **Automated Data Collection**: AI agents continuously monitor competitor websites, pricing, and inventory
- **Real-Time Benchmarking**: Instant competitive positioning with rankings and deltas
- **Actionable Insights**: Executive-ready reports showing exactly how you stack up
- **Weekly Intelligence**: Fresh competitive landscape analysis delivered automatically

---

## 💼 Business Value Proposition

### For Sales Leadership
- **Proactive Pricing Strategy**: Know competitor moves before they impact your sales
- **Market Positioning**: Clear visibility into where you rank vs. competition
- **Inventory Intelligence**: Track competitor quick possession availability and pricing

### For Sales Teams
- **Objection Handling**: Data-backed responses to price and feature comparisons
- **Competitive Talking Points**: Know exactly how your specs and pricing compare
- **Market Awareness**: Stay informed on new promotions and competitor changes

### ROI Impact
- **Time Savings**: Eliminate 5-10 hours/week of manual competitive research
- **Faster Response**: Reduce lag time from competitor move to internal action
- **Better Decisions**: Data-driven pricing and positioning strategies
- **Margin Protection**: Early warning system for competitive pressure

---

## 🚀 Quick Demo Setup

### Prerequisites
```bash
# Python 3.8+
python --version

# Virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

### 1-Minute Installation
```bash
# Clone and setup
git clone [repository-url]
cd mondaymorning
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys (OpenAI required for demo)

# Run demo pipeline
python run.py pipeline properties
```

### Demo Output
The pipeline generates:
- **HTML Report**: `outputs/reports/properties_report_[timestamp].html`
- **PDF Export**: `outputs/reports/properties_report_[timestamp].pdf`
- **Data Tables**: Interactive competitor comparison and executive summary

---

## 📊 Demo Walkthrough

### Business Demo Script (8 minutes)

#### 1. Problem Setup (2 min)
*"Traditional competitive analysis in homebuilding is manual, reactive, and time-consuming. By the time you adjust pricing or positioning, market opportunities are already lost."*

#### 2. Live Pipeline Demo (3 min)
```bash
# Show the automation magic
python run.py pipeline properties
```

**Narration**: *"This command automatically collects data from competitor websites, normalizes pricing and inventory information, performs competitive benchmarking, and generates executive-ready reports - all in under 2 minutes."*

#### 3. Report Analysis (3 min)
Open the generated HTML report and walk through:

- **Executive Summary Cards**: 
  - *"We currently have X active listings vs. market average of Y"*
  - *"Our median price per sqft ranks #3 out of 6 competitors"*
  - *"We're covering 8 communities compared to competitor average of 5"*

- **Competitive Comparison Table**:
  - *"Here's exactly how we stack up - rankings, pricing deltas, inventory positions"*
  - *"Competitor A is $50K (7.2%) above our pricing in the same market"*
  - *"We can see market gaps and positioning opportunities immediately"*

#### 4. Business Impact (1 min)
*"This same analysis, done manually, would take your team 6-8 hours and be outdated by the time it's complete. With MondayMorning, you get fresh competitive intelligence weekly, enabling proactive rather than reactive decision-making."*

---

## ⚙️ Platform Architecture

### Data Pipeline Overview
```
Collect → Normalize → Analyze → Report
   ↓         ↓          ↓        ↓
Raw Data → Clean Data → Insights → Reports
```

### Current Data Sources
- **Quick Possession Properties**: Automated website scraping with AI extraction
- **Google Reviews**: API-based competitor review analysis
- **Digital Advertising**: Competitor promotion and campaign monitoring *(roadmap)*
- **News Articles**: Industry and competitor mention tracking *(roadmap)*

### Competitive Intelligence Features
- **Automatic Benchmarking**: Rankings and competitive deltas on all metrics
- **Market Positioning**: Know exactly where you stand vs. true competitors
- **Change Detection**: Identify new listings, price changes, sold properties
- **Executive Summaries**: Business-ready insights with competitive context

---

## 🛠️ Usage Guide

### Individual Components
```bash
# Data collection
python run.py run collect quickpossession

# Data normalization  
python run.py run normalize properties

# Analysis generation
python run.py run analyze properties_table
python run.py run analyze properties_snapshot

# Report creation
python run.py run report properties
```

### Pipeline Automation
```bash
# Full properties pipeline
python run.py pipeline properties

# Custom stage selection
python run.py pipeline properties --stages analyze,report

# List available components
python run.py list
```

### Configuration
```bash
# Environment variables (.env)
OUR_COMPANY_ID=ABC           # Your company identifier for benchmarking
OPENAI_API_KEY=sk-...        # Required for AI data extraction
DATAFORSEO_USERNAME=...      # For review data collection
DATAFORSEO_PASSWORD=...      # For review data collection

# Competitor configuration (competitors.csv)
competitor_id,name,website,cid
COMP1,Competitor One,example1.com,1234567890
COMP2,Competitor Two,example2.com,0987654321
```

---

## 📁 Project Structure

```
├── src/
│   ├── collect/              # Data collection modules
│   ├── normalize/            # Data cleaning and validation
│   ├── analyze/              # Competitive analysis and benchmarking
│   └── report/               # Report generation and templates
├── data/
│   ├── raw/                  # Collected data (timestamped)
│   ├── clean/                # Normalized master databases
│   └── enriched/             # Analysis results
├── outputs/
│   └── reports/              # Generated HTML/PDF reports
├── templates/                # Report templates (Jinja2)
├── run.py                    # Main CLI interface
└── requirements.txt          # Python dependencies
```

---

## 🔧 Development Workflow

### Adding New Data Sources

1. **Create Collector** (`src/collect/new_source.py`)
   - Inherit from `BaseCollector`
   - Implement data collection logic
   - Add to collector registry

2. **Create Normalizer** (`src/normalize/new_normalizer.py`)
   - Inherit from `BaseNormalizer`
   - Define data validation and enrichment
   - Add to normalizer registry

3. **Create Analyzers** (`src/analyze/new_analyzer.py`)
   - Inherit from `BaseAnalyzer` (includes automatic benchmarking)
   - Define focused analysis metrics
   - Configure competitive benchmarking

4. **Update Pipeline**
   - Add to pipeline configuration in `run.py`
   - Test end-to-end workflow

### Testing
```bash
# Stage-specific testing
python test_collect.py
python test_normalize.py
python test_analyze.py
python test_report.py

# Debug utilities
python debug_csv.py          # Inspect data structure
```

---

## 🎯 Roadmap & Next Steps

### Phase 1: MVP Enhancement
- **Reviews Intelligence**: Customer sentiment and rating analysis
- **Email Alerts**: Automated notifications for significant competitor changes
- **Historical Trending**: Track competitor movements over time

### Phase 2: Advanced Analytics
- **Predictive Insights**: Market trend forecasting and opportunity identification
- **Geospatial Analysis**: Community-level competitive heat maps
- **Price Optimization**: AI-driven pricing recommendations

### Phase 3: Enterprise Integration
- **CRM Integration**: Push competitive insights directly to sales systems
- **API Development**: Enable integration with existing business intelligence tools
- **Multi-Market Support**: Expand beyond Calgary to other metropolitan areas

---

## 🏗️ Industry Context

### Target Market
- **Primary**: New home builders with quick possession inventory
- **Geographic**: Calgary, Alberta metropolitan area
- **Focus**: Competitive pricing and inventory intelligence

### Competitive Landscape
- **Traditional Methods**: Manual website checking, sporadic market reports
- **Current Tools**: Basic web scraping, Excel-based analysis
- **MondayMorning Advantage**: AI-powered automation with business intelligence

---

## 📋 Demo Preparation Checklist

### Technical Setup
- [ ] Virtual environment activated
- [ ] All dependencies installed (`pip install -r requirements.txt`)
- [ ] OpenAI API key configured in `.env`
- [ ] Sample competitor data available
- [ ] Pipeline runs successfully end-to-end
- [ ] Report generates without errors

### Demo Environment
- [ ] Backup static report available (in case of live demo issues)
- [ ] Report opens cleanly in browser
- [ ] Mobile/tablet compatibility verified for presentation flexibility
- [ ] Demo narrative practiced and timed

### Business Readiness
- [ ] Competitive insights are meaningful and accurate
- [ ] Value proposition clearly articulated
- [ ] ROI calculation prepared
- [ ] Next steps and implementation timeline defined

---

## 📞 Support & Contact

### Technical Issues
- Review logs in console output for specific error messages
- Check data availability in respective directories (`data/raw/`, `data/clean/`)
- Verify API keys and environment configuration

### Business Inquiries
- Schedule demo: [contact information]
- Implementation planning: [contact information]
- Custom development: [contact information]

---

## 📄 License & Status

**Status**: Demo-Ready Prototype  

---

*MondayMorning Platform - Transform manual competitive analysis into automated competitive advantage.*