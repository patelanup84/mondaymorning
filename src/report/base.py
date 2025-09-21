from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import logging
import warnings
from jinja2 import Environment, FileSystemLoader
import weasyprint

from src.config import ENRICHED_DIR, REPORTS_DIR


class BaseReport(ABC):
    """Abstract base class for report generation modules."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"report.{name}")
        self.template_env = self._setup_templates()
    
    def render(self) -> Optional[Path]:
        """Main report generation pipeline."""
        try:
            self.logger.info(f"Starting {self.name} report generation")
            
            # Suppress verbose logging during report generation
            original_levels = {}
            verbose_loggers = [
                'fontTools', 'fontTools.subset', 'fontTools.ttLib', 
                'fontTools.ttLib.ttFont', 'weasyprint'
            ]
            
            for logger_name in verbose_loggers:
                logger = logging.getLogger(logger_name)
                original_levels[logger_name] = logger.level
                logger.setLevel(logging.CRITICAL)
            
            try:
                # Load analysis data
                data = self._load_analysis_data()
                if not data:
                    self.logger.warning("No analysis data available for report")
                    return None
                
                # Prepare template context
                context = self._prepare_context(data)
                
                # Render HTML report
                html_path = self._render_html(context)
                
                # Export PDF
                pdf_path = self._export_pdf(html_path)
                
                self.logger.info(f"Report generated: {html_path}")
                return html_path
                
            finally:
                # Restore original log levels
                for logger_name, original_level in original_levels.items():
                    logging.getLogger(logger_name).setLevel(original_level)
            
        except Exception as e:
            self.logger.error(f"Report generation failed: {e}")
            return None
    
    @abstractmethod
    def _load_analysis_data(self) -> Optional[Dict[str, pd.DataFrame]]:
        """Load required analysis data from enriched directory."""
        pass
    
    @abstractmethod
    def _get_template_name(self) -> str:
        """Get the name of the Jinja template file."""
        pass
    
    def _setup_templates(self) -> Environment:
        """Setup Jinja2 template environment."""
        template_dir = Path(__file__).resolve().parents[2] / "templates"
        template_dir.mkdir(exist_ok=True)
        
        env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=True
        )
        return env
    
    def _prepare_context(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Prepare template context from analysis data."""
        context = {
            'report_title': f"{self.name.replace('_', ' ').title()} Report",
            'generated_at': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
            'data': data
        }
        
        # Convert DataFrames to template-friendly formats
        for key, df in data.items():
            if df is not None and not df.empty:
                context[f"{key}_records"] = df.to_dict('records')
                context[f"{key}_columns"] = list(df.columns)
        
        return context
    
    def _render_html(self, context: Dict[str, Any]) -> Path:
        """Render HTML report from template."""
        template_name = self._get_template_name()
        template = self.template_env.get_template(template_name)
        
        html_content = template.render(**context)
        
        # Save HTML file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        html_filename = f"{self.name}_{timestamp}.html"
        html_path = REPORTS_DIR / html_filename
        
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return html_path
    
    def _export_pdf(self, html_path: Path) -> Optional[Path]:
        """Export HTML report to PDF."""
        try:
            pdf_path = html_path.with_suffix('.pdf')
            
            # Suppress all warnings and debug logging during PDF generation
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                # Store original log levels
                original_levels = {}
                loggers_to_suppress = [
                    'weasyprint', 'fontTools', 'fontTools.subset', 
                    'fontTools.ttLib', 'fontTools.ttLib.ttFont'
                ]
                
                for logger_name in loggers_to_suppress:
                    logger = logging.getLogger(logger_name)
                    original_levels[logger_name] = logger.level
                    logger.setLevel(logging.CRITICAL)
                
                try:
                    # Use weasyprint for PDF generation
                    weasyprint.HTML(filename=str(html_path)).write_pdf(str(pdf_path))
                finally:
                    # Restore original log levels
                    for logger_name, original_level in original_levels.items():
                        logging.getLogger(logger_name).setLevel(original_level)
            
            self.logger.info(f"PDF exported: {pdf_path}")
            return pdf_path
            
        except Exception as e:
            self.logger.warning(f"PDF export failed: {e}")
            return None
    
    def _load_parquet_safe(self, filename: str) -> Optional[pd.DataFrame]:
        """Safely load parquet file from enriched directory."""
        file_path = ENRICHED_DIR / filename
        
        if not file_path.exists():
            self.logger.warning(f"Analysis file not found: {filename}")
            return None
        
        try:
            df = pd.read_parquet(file_path)
            self.logger.debug(f"Loaded {len(df)} records from {filename}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load {filename}: {e}")
            return None