# **MondayMorning Platform**

MondayMorning automatically collects, analyzes, and benchmarks competitive data to provide actionable intelligence for strategic decision-making. The platform transforms manual competitive research into automated insights with built-in benchmarking against your position in the market.

## **Core Concept**

**Automated Intelligence Pipeline**: Collect competitor data → Normalize and validate → Analyze with benchmarking → Generate insights

**Competitive Benchmarking**: Every analysis automatically includes rankings, deltas, and positioning relative to your company, providing clear "how do we stack up" answers.

**Hyper-Relevant Insights**: Focused analysis modules generate specific intelligence for different business needs rather than generic market reports.

## **Architecture**

### **4-Stage Pipeline**

Collect → Normalize → Analyze → Report

Collect: AI agents gather data from websites, APIs, and documents with intelligent state management.  
Normalize: Clean, validate, and merge data into canonical databases.  
Analyze: Compute competitive metrics with automatic benchmarking.  
Report: Generate interactive reports and executive summaries.

### **Data Flow**

Raw Sources → State Management → Master Databases → Analysis Results → Intelligence Reports  
data/raw/   → data/clean/        → data/enriched/  → outputs/reports/

## **Quick Start**

### **Installation**

git clone \[repository\]  
cd mondaymorning  
pip install \-r requirements.txt  
cp .env.example .env  
\# Edit .env with your API keys

### **Configuration**

**Environment (.env)** \- Secrets only:

OPENAI\_API\_KEY=sk-...        \# For AI data extraction  
DATAFORSEO\_USERNAME=...      \# For Google Reviews collector  
DATAFORSEO\_PASSWORD=...      \# For Google Reviews collector

**Business Settings** \- Configured in code:

* Company ID: src/models.py (OUR\_COMPANY\_ID \= "PRM")  
* Collector defaults: src/models.py (URL limits, LLM providers, locations)  
* System paths: src/config.py (data directories, logging)

**Competitors (competitors.csv)**:

competitor\_id,name,domain,cid  
YOUR\_CODE,Your Company,yourcompany.com,1234567890  
COMP1,Competitor One,comp1.com,2345678901  
COMP2,Competitor Two,comp2.com,3456789012

### **Basic Usage**

\# Full pipeline  
python run.py pipeline properties

\# Individual stages  
python run.py run collect quickpossession  
python run.py run normalize quickpossession  
python run.py run analyze properties\_table  
python run.py run report properties

\# List available components  
python run.py list

## **Workflow**

### **1\. Data Collection**

Automated agents collect competitor information with intelligent caching:

* **Web Scraping**: Extract structured data from competitor websites.  
* **API Integration**: Gather data from review platforms and business directories.  
* **Document Processing**: Parse PDFs, reports, and marketing materials.  
* **Smart Caching**: The QuickPossession collector uses SQLite-based state management to track URL discovery and extraction progress, enabling incremental data collection with 24-hour freshness checking and fault tolerance.

### **2\. Data Normalization**

Raw data is cleaned and standardized:

* **Schema Validation**: Ensure data quality and consistency.  
* **Enrichment**: Add derived fields and calculated metrics.  
* **Master Databases**: Maintain canonical datasets with change tracking.

### **3\. Competitive Analysis**

Generate focused intelligence with built-in benchmarking:

* **Automatic Rankings**: See how you rank across key metrics.  
* **Competitive Deltas**: Understand gaps and opportunities.  
* **Market Positioning**: Know exactly where you stand.

### **4\. Intelligence Reports**

Deliver actionable insights:

* **Executive Summaries**: Key metrics with competitive context.  
* **Detailed Analysis**: Interactive tables and comparative data.  
* **Export Options**: HTML reports and PDF exports.

## **Architecture Details**

### **Configuration Architecture**

Clean separation of concerns:

* **.env**: API keys and secrets only  
* **src/config.py**: System constants (paths, directories, logging)  
* **src/models.py**: Business logic defaults (company ID, collector settings)

### **Modular Design**

Each stage uses base classes with inheritance for extensibility:

* BaseCollector → Specific collectors (web, API, document)  
* BaseNormalizer → Data type normalizers (properties, reviews)  
* BaseAnalyzer → Analysis modules (tables, snapshots, trends)  
* BaseReport → Report generators (HTML, PDF, dashboards)  
* SQLiteStateManager → Generic database utility for persistent state management

### **Competitive Benchmarking**

Built into every analyzer automatically:

\# Automatic features in all analysis  
\- Rankings: "2nd out of 5 competitors"  
\- Deltas: "$50K (7.2%) above us"  
\- Context: "Top 25th percentile in market"

### **Data Storage**

* **SQLite**: Primary state management for incremental collection and data handoff to the normalization stage.  
* **Parquet**: Primary storage for master databases (/clean) and analysis results (/enriched). Optimized for high-performance analysis.  
* **CSV**: Human-readable exports of master databases and analysis results for easy inspection.  
* **Timestamped Files**: Track changes and maintain history in logs and reports.

## **Adding New Intelligence Sources**

### **1\. Create Collector**

\# src/collect/new\_source.py  
class NewSourceCollector(BaseCollector):  
    def \_\_init\_\_(self):  
        super().\_\_init\_\_("new\_source")  
        self.state\_manager \= SQLiteStateManager("new\_source")  \# Optional state management  
      
    async def \_collect\_raw(self, config):  
        \# Collection logic with optional caching  
      
    async def \_transform(self, config):  
        \# Schema compliance

### **2\. Create Normalizer**

\# src/normalize/new\_normalizer.py  
class NewNormalizer(BaseNormalizer):  
    def \_load\_raw\_data(self, collector\_name: str):  
        \# Logic to load data from collector's source (e.g., SQLite, a different CSV)  
      
    def \_validate(self, df):  
        \# Data validation  
      
    def \_enrich(self, df):  
        \# Add derived fields  
      
    def \_merge(self, new\_df, master\_path):  
        \# Update master database

### **3\. Create Analyzer**

\# src/analyze/new\_analyzer.py  
class NewAnalyzer(BaseAnalyzer):  
    def \_load\_data(self):  
        \# Load from master database  
      
    def \_compute\_analysis(self, data):  
        \# Generate focused metrics  
      
    def \_get\_benchmark\_config(self):  
        \# Configure competitive benchmarking

### **4\. Update Pipeline**

Add to pipeline configuration and test end-to-end workflow.

## **CLI Reference**

### **Individual Components**

python run.py run \<stage\> \<component\>

\# Examples  
python run.py run collect quickpossession  
python run.py run normalize quickpossession  
python run.py run analyze properties\_table  
python run.py run report properties

### **Pipeline Automation**

python run.py pipeline \<data\_type\> \[--stages stage1,stage2,...\]

\# Examples  
python run.py pipeline properties  
python run.py pipeline reviews \--stages analyze,report

### **Utility Commands**

python run.py list                    \# Show all available components  
python run.py \--verbose \<command\>     \# Detailed console logging  
python run.py \--no-log \<command\>      \# Disable file logging

## **Project Structure**

├── src/  
│   ├── collect/          \# Data collection modules  
│   ├── normalize/        \# Data cleaning and validation    
│   ├── analyze/          \# Competitive analysis  
│   ├── report/           \# Intelligence report generation  
│   └── utils/            \# Shared utilities (SQLite manager, etc.)  
├── data/  
│   ├── raw/              \# Collected data (SQLite state, CSV exports, JSON, etc.)  
│   ├── clean/            \# Master databases  
│   └── enriched/         \# Analysis results  
├── logs/                 \# Session logs (timestamped)  
├── outputs/reports/      \# Generated reports  
├── templates/            \# Report templates  
├── competitors.csv       \# Competitor configuration  
├── run.py               \# CLI interface  
└── requirements.txt     \# Dependencies

## **Intelligence Outputs**

### **Analysis Results**

* **Competitive Tables**: Direct competitor comparisons with rankings  
* **Executive Snapshots**: Key metrics with market positioning  
* **Trend Analysis**: Changes and movements over time  
* **Market Insights**: Opportunities and competitive gaps

### **Report Formats**

* **Interactive HTML**: Sortable tables, responsive design  
* **PDF Exports**: Print-ready executive reports  
* **Data Exports**: CSV/Excel for further analysis  
* **API Access**: Programmatic access to intelligence data

*MondayMorning Platform \- Automated competitive intelligence for strategic advantage.*