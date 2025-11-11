#!/bin/bash
# Helper script to switch between scraping modes for task t6

MODE=${1:-help}

case $MODE in
  daily)
    echo "Switching to DAILY ITERATION mode..."
    # Ensure the playbook doesn't have nom_region
    jq '.tasks = [.tasks[] | if .id == "t6" then del(.params.nom_region) else . end]' \
      agent_scrape_playbook_loire_bretagne.json > /tmp/playbook.json && \
    mv /tmp/playbook.json agent_scrape_playbook_loire_bretagne.json
    echo "✓ Daily iteration mode activated"
    echo "  Run: python main.py --tasks t6"
    ;;

  region)
    REGION=${2:-Bretagne}
    echo "Switching to REGION FILTER mode (region: $REGION)..."
    # Add nom_region to t6 params
    jq --arg region "$REGION" '.tasks = [.tasks[] | if .id == "t6" then .params.nom_region = $region else . end]' \
      agent_scrape_playbook_loire_bretagne.json > /tmp/playbook.json && \
    mv /tmp/playbook.json agent_scrape_playbook_loire_bretagne.json
    echo "✓ Region filter mode activated for: $REGION"
    echo "  Run: python main.py --tasks t6"
    ;;

  status)
    echo "Current t6 configuration:"
    jq '.tasks[] | select(.id=="t6")' agent_scrape_playbook_loire_bretagne.json

    if jq -e '.tasks[] | select(.id=="t6") | .params.nom_region' agent_scrape_playbook_loire_bretagne.json > /dev/null 2>&1; then
      REGION=$(jq -r '.tasks[] | select(.id=="t6") | .params.nom_region' agent_scrape_playbook_loire_bretagne.json)
      echo ""
      echo "Mode: REGION FILTER ($REGION)"
    else
      echo ""
      echo "Mode: DAILY ITERATION"
    fi
    ;;

  help|*)
    cat << 'EOF'
Usage: ./switch_mode.sh [command] [options]

Commands:
  daily               Switch to daily iteration mode (comprehensive)
  region [name]       Switch to region filter mode (default: Bretagne)
  status              Show current mode configuration
  help                Show this help message

Examples:
  ./switch_mode.sh daily                 # Use daily iteration
  ./switch_mode.sh region Bretagne       # Filter for Bretagne region
  ./switch_mode.sh region "Pays de la Loire"  # Filter for Pays de la Loire
  ./switch_mode.sh status                # Check current mode

After switching, run:
  python main.py --tasks t6
EOF
    ;;
esac
