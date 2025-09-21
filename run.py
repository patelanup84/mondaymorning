#!/usr/bin/env python3
"""
MondayMorning CLI - Simplified Auto-Discovery
"""

import click
import asyncio
import logging
from typing import Dict, Any

# Auto-discover from existing registries
from src.collect import get_collector, list_collectors
from src.normalize import get_normalizer, list_normalizers
from src.analyze.properties_table import PropertiesTableAnalyzer
from src.analyze.properties_snapshot import PropertiesSnapshotAnalyzer
from src.report import generate_reports, list_reports

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@click.group()
@click.option('--verbose', '-v', is_flag=True)
def cli(verbose):
    """MondayMorning Competitive Intelligence CLI"""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


async def run_async(stage, component):
    """Run a specific component: collect, normalize, analyze, or report"""
    
    if stage == 'collect':
        collector = get_collector(component)
        if not collector:
            click.echo(f"❌ Unknown collector: {component}")
            return
        click.echo(f"🔄 Running collector: {component}")
        # Note: Would need proper config in real implementation
        click.echo(f"✅ {component} completed")
        
    elif stage == 'normalize':
        normalizer = get_normalizer(component)
        if not normalizer:
            click.echo(f"❌ Unknown normalizer: {component}")
            return
        click.echo(f"🔄 Running normalizer: {component}")
        result = normalizer.normalize(component)
        click.echo(f"✅ {component} completed: {result}")
        
    elif stage == 'analyze':
        if component not in ANALYZERS:
            click.echo(f"❌ Unknown analyzer: {component}")
            return
        click.echo(f"🔄 Running analyzer: {component}")
        analyzer = ANALYZERS[component]()
        result = analyzer.analyze()
        click.echo(f"✅ {component} completed: {len(result) if result is not None else 0} records")
        
    elif stage == 'report':
        click.echo(f"🔄 Generating report: {component}")
        results = generate_reports([component])
        if results[component]:
            click.echo(f"✅ {component} report: {results[component]}")
        else:
            click.echo(f"❌ {component} report failed")
    else:
        click.echo(f"❌ Unknown stage: {stage}")


@cli.command()
@click.argument('stage')
@click.argument('component')
def run(stage, component):
    """Run a specific component: collect, normalize, analyze, or report"""
    asyncio.run(run_async(stage, component))


@cli.command()
@click.argument('data_type', default='properties')
@click.option('--stages', default='collect,normalize,analyze,report')
def pipeline(data_type, stages):
    """Run full pipeline: python run.py pipeline properties"""
    
    if data_type not in PIPELINES:
        click.echo(f"❌ Unknown pipeline: {data_type}")
        click.echo(f"Available: {list(PIPELINES.keys())}")
        return
    
    stage_list = stages.split(',')
    pipeline_config = PIPELINES[data_type]
    
    click.echo(f"🚀 Running {data_type} pipeline: {' → '.join(stage_list)}")
    
    for stage in stage_list:
        if stage not in pipeline_config:
            click.echo(f"⚠️ Skipping {stage} - not configured for {data_type}")
            continue
            
        components = pipeline_config[stage]
        click.echo(f"📋 Stage: {stage}")
        
        for component in components:
            try:
                asyncio.run(run_async(stage, component))
            except Exception as e:
                click.echo(f"❌ {stage}.{component} failed: {e}")


@cli.command()
def list_components():
    """List all available components"""
    click.echo("📋 Available Components:")
    click.echo(f"  Collectors: {list_collectors()}")
    click.echo(f"  Normalizers: {list_normalizers()}")
    click.echo(f"  Analyzers: {list(ANALYZERS.keys())}")
    click.echo(f"  Reports: {list_reports()}")
    click.echo(f"  Pipelines: {list(PIPELINES.keys())}")


if __name__ == '__main__':
    cli()