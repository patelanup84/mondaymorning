#!/usr/bin/env python3
"""
MondayMorning CLI - Simplified Auto-Discovery
"""

import click
import asyncio
import logging
import time
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Auto-discover from existing registries
from src.collect import get_collector, list_collectors
from src.normalize import get_normalizer, list_normalizers
from src.analyze.properties_table import PropertiesTableAnalyzer
from src.analyze.properties_snapshot import PropertiesSnapshotAnalyzer
from src.report import generate_reports, list_reports
from src.config import setup_logging, load_competitors_from_csv, RAW_DIR

# Registry mapping for auto-discovery
ANALYZERS = {
    'properties_table': PropertiesTableAnalyzer,
    'properties_snapshot': PropertiesSnapshotAnalyzer,
}

PIPELINES = {
    'properties': {
        'collect': ['quickpossession'],
        'normalize': ['properties'],
        'analyze': ['properties_table', 'properties_snapshot'],
        'report': ['properties']
    }
}

# Global session tracking
session_start_time = None
session_log_file = None
command_history = []


@click.group()
@click.option('--verbose', '-v', is_flag=True)
@click.option('--no-log', is_flag=True, help='Disable file logging')
def cli(verbose, no_log):
    """MondayMorning Competitive Intelligence CLI"""
    global session_start_time, session_log_file
    
    # Initialize logging
    if not no_log:
        session_log_file = setup_logging(verbose=verbose)
    
    # Set up basic logging if file logging disabled
    if no_log:
        setup_logging(None, verbose)
    
    # Track session start
    session_start_time = time.time()
    logger = logging.getLogger(__name__)
    
    # Log command line arguments
    cmd_args = ' '.join(sys.argv[1:])
    logger.info(f"Command: python run.py {cmd_args}")
    logger.info(f"Session started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Store command in history
    command_history.append({
        'timestamp': datetime.now().isoformat(),
        'command': cmd_args,
        'args': sys.argv[1:]
    })


async def run_async(stage, component):
    """Run a specific component: collect, normalize, analyze, or report"""
    logger = logging.getLogger(__name__)
    stage_start_time = time.time()
    
    try:
        logger.info(f"Starting {stage}.{component}")
        
        if stage == 'collect':
            collector = get_collector(component)
            if not collector:
                logger.error(f"Unknown collector: {component}")
                click.echo(f"❌ Unknown collector: {component}")
                return
            
            logger.info(f"Running collector: {component}")
            click.echo(f"🔄 Running collector: {component}")
            
            # Load competitors and create proper config
            logger.info("Loading competitors from CSV...")
            competitors = load_competitors_from_csv()
            logger.info(f"Loaded {len(competitors)} competitors from CSV")
            
            if not competitors:
                logger.error("Failed to load competitors from CSV")
                click.echo(f"❌ Failed to load competitors from CSV")
                return
            
            # Debug: Log competitor details
            for i, comp in enumerate(competitors):
                logger.info(f"Competitor {i+1}: {comp}")
            
            # Create collector config based on component type
            if component == 'quickpossession':
                from src.models import QPCollectorConfig
                logger.info("Creating QPCollectorConfig...")
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                config = QPCollectorConfig(
                    competitors=competitors,
                    output_path=RAW_DIR / f"{timestamp}_{component}.csv"
                )
                logger.info(f"Config created: url_limit={config.url_limit_per_competitor}, llm_provider={config.llm_provider}")
                logger.info(f"Output path: {config.output_path}")
            else:
                logger.error(f"Unsupported collector type: {component}")
                click.echo(f"❌ Unsupported collector type: {component}")
                return
            
            # Actually run the collector
            result = await collector.collect(config)
            
            # Check success based on metadata
            successful = result.metadata.get('successful', 0)
            total_attempted = result.metadata.get('total_attempted', 0)
            
            if successful > 0:
                click.echo(f"✅ {component} completed: {successful} URLs discovered")
                logger.info(f"Collector {component} completed successfully: {result.metadata}")
            else:
                click.echo(f"❌ {component} failed: {result.metadata.get('errors', [])}")
                logger.error(f"Collector {component} failed: {result.metadata.get('errors', [])}")
            
        elif stage == 'normalize':
            normalizer = get_normalizer(component)
            if not normalizer:
                logger.error(f"Unknown normalizer: {component}")
                click.echo(f"❌ Unknown normalizer: {component}")
                return
            logger.info(f"Running normalizer: {component}")
            click.echo(f"🔄 Running normalizer: {component}")
            result = normalizer.normalize(component)
            click.echo(f"✅ {component} completed: {result}")
            
        elif stage == 'analyze':
            if component not in ANALYZERS:
                logger.error(f"Unknown analyzer: {component}")
                click.echo(f"❌ Unknown analyzer: {component}")
                return
            logger.info(f"Running analyzer: {component}")
            click.echo(f"🔄 Running analyzer: {component}")
            analyzer = ANALYZERS[component]()
            result = analyzer.analyze()
            click.echo(f"✅ {component} completed: {len(result) if result is not None else 0} records")
            
        elif stage == 'report':
            logger.info(f"Generating report: {component}")
            click.echo(f"🔄 Generating report: {component}")
            results = generate_reports([component])
            if results[component]:
                click.echo(f"✅ {component} report: {results[component]}")
            else:
                click.echo(f"❌ {component} report failed")
        else:
            logger.error(f"Unknown stage: {stage}")
            click.echo(f"❌ Unknown stage: {stage}")
            
        # Log stage completion
        stage_duration = time.time() - stage_start_time
        logger.info(f"Completed {stage}.{component} in {stage_duration:.2f} seconds")
        
    except Exception as e:
        stage_duration = time.time() - stage_start_time
        logger.error(f"Failed {stage}.{component} after {stage_duration:.2f} seconds: {e}", exc_info=True)
        click.echo(f"❌ {stage}.{component} failed: {e}")


@cli.command()
@click.argument('stage')
@click.argument('component')
def run(stage, component):
    """Run a specific component: collect, normalize, analyze, or report"""
    logger = logging.getLogger(__name__)
    logger.info(f"Executing command: run {stage} {component}")
    asyncio.run(run_async(stage, component))


@cli.command()
@click.argument('data_type', default='properties')
@click.option('--stages', default='collect,normalize,analyze,report')
def pipeline(data_type, stages):
    """Run full pipeline: python run.py pipeline properties"""
    logger = logging.getLogger(__name__)
    logger.info(f"Executing command: pipeline {data_type} --stages {stages}")
    
    pipeline_start_time = time.time()
    
    if data_type not in PIPELINES:
        logger.error(f"Unknown pipeline: {data_type}")
        click.echo(f"❌ Unknown pipeline: {data_type}")
        click.echo(f"Available: {list(PIPELINES.keys())}")
        return
    
    stage_list = stages.split(',')
    pipeline_config = PIPELINES[data_type]
    
    logger.info(f"Running {data_type} pipeline with stages: {stage_list}")
    click.echo(f"🚀 Running {data_type} pipeline: {' → '.join(stage_list)}")
    
    for stage in stage_list:
        if stage not in pipeline_config:
            logger.warning(f"Skipping {stage} - not configured for {data_type}")
            click.echo(f"⚠️ Skipping {stage} - not configured for {data_type}")
            continue
            
        components = pipeline_config[stage]
        logger.info(f"Processing stage: {stage} with components: {components}")
        click.echo(f"📋 Stage: {stage}")
        
        for component in components:
            try:
                asyncio.run(run_async(stage, component))
            except Exception as e:
                logger.error(f"Pipeline stage {stage}.{component} failed: {e}", exc_info=True)
                click.echo(f"❌ {stage}.{component} failed: {e}")
    
    # Log pipeline completion
    pipeline_duration = time.time() - pipeline_start_time
    logger.info(f"Pipeline completed in {pipeline_duration:.2f} seconds")


@cli.command()
def list_components():
    """List all available components"""
    logger = logging.getLogger(__name__)
    logger.info("Executing command: list-components")
    
    click.echo("📋 Available Components:")
    click.echo(f"  Collectors: {list_collectors()}")
    click.echo(f"  Normalizers: {list_normalizers()}")
    click.echo(f"  Analyzers: {list(ANALYZERS.keys())}")
    click.echo(f"  Reports: {list_reports()}")
    click.echo(f"  Pipelines: {list(PIPELINES.keys())}")


def log_session_end():
    """Log session end with summary information."""
    global session_start_time, command_history
    
    if session_start_time is None:
        return
        
    logger = logging.getLogger(__name__)
    session_duration = time.time() - session_start_time
    
    logger.info("=" * 80)
    logger.info(f"MondayMorning Session Ended - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total session duration: {session_duration:.2f} seconds")
    logger.info(f"Commands executed: {len(command_history)}")
    
    if command_history:
        logger.info("Command history:")
        for i, cmd in enumerate(command_history, 1):
            logger.info(f"  {i}. {cmd['timestamp']}: {cmd['command']}")
    
    logger.info("=" * 80)


if __name__ == '__main__':
    try:
        cli()
    finally:
        # Always log session end, even if CLI fails
        log_session_end()