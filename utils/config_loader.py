import yaml
from pathlib import Path

# This utility provides a single function to load our YAML configuration,
# creating a central source of truth for all pipeline settings.
# This follows our 'Mindful DRY' and 'Modular and Configurable' principles.

def load_config(
    config_path: str = "config.yaml",
    competitors_path: str = "competitors.yaml"
) -> dict:
    """
    Loads and merges the main config and competitors YAML files from the project root.

    Args:
        config_path (str): The name of the main configuration file.
        competitors_path (str): The name of the competitors configuration file.

    Returns:
        dict: A single dictionary containing all configuration settings.
    """
    # Assume this script is in `utils/`, so the project root is one level up.
    project_root = Path(__file__).parent.parent
    
    config_file = project_root / config_path
    competitors_file = project_root / competitors_path

    # Fail-Fast if config files are not found.
    assert config_file.exists(), f"Config file not found at: {config_file}"
    assert competitors_file.exists(), f"Competitors file not found at: {competitors_file}"

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    with open(competitors_file, 'r') as f:
        competitors_config = yaml.safe_load(f)
    
    # Merge competitors into the main config for a single, unified source.
    config.update(competitors_config)
    
    return config

# This block allows us to test the utility independently to ensure it works.
if __name__ == '__main__':
    print("Testing configuration loader...")
    try:
        cfg = load_config()
        # Assertions to validate the loaded structure
        assert "pipeline" in cfg
        assert "database" in cfg
        assert "competitors" in cfg
        assert "PRM" in cfg["competitors"]
        
        print("✅ Configuration loaded successfully.")
        import json
        print("\n--- Loaded Config ---")
        print(json.dumps(cfg, indent=2))
        print("---------------------\n")

    except AssertionError as e:
        print(f"❌ Test Failed: {e}")