#!/bin/bash

# tree.sh - A script to display directory structure like the tree command
# Usage: ./tree.sh [OPTIONS] [directory]

# Default values
SHOW_FILES=true
SHOW_DIRS=true
MAX_DEPTH=-1
START_DIR="."

# Colors
COLOR_DIR="\033[1;34m"    # Blue
COLOR_FILE="\033[0m"      # Default
COLOR_RESET="\033[0m"

# Unicode characters for tree structure
PIPE="│   "
TEE="├── "
ELBOW="└── "
BLANK="    "

# Counters
DIR_COUNT=0
FILE_COUNT=0

# Function to display usage
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS] [directory]

Display directory structure in a tree-like format.

OPTIONS:
    -d          Show directories only
    -f          Show files only
    -L <level>  Maximum depth to traverse (default: unlimited)
    -h          Show this help message
    
EXAMPLES:
    $(basename "$0")                 # Show current directory
    $(basename "$0") /path/to/dir    # Show specific directory
    $(basename "$0") -d              # Show only directories
    $(basename "$0") -f              # Show only files
    $(basename "$0") -L 2            # Show up to 2 levels deep

EOF
    exit 0
}

# Function to print tree structure recursively
print_tree() {
    local dir="$1"
    local prefix="$2"
    local depth="$3"
    
    # Check max depth
    if [[ $MAX_DEPTH -ne -1 && $depth -gt $MAX_DEPTH ]]; then
        return
    fi
    
    # Get list of items, excluding hidden files by default
    local items=()
    while IFS= read -r -d '' item; do
        items+=("$item")
    done < <(find "$dir" -maxdepth 1 -mindepth 1 ! -name ".*" -print0 2>/dev/null | sort -z)
    
    local count=${#items[@]}
    local index=0
    
    for item in "${items[@]}"; do
        local basename=$(basename "$item")
        index=$((index + 1))
        
        # Determine if this is the last item
        local is_last=false
        if [[ $index -eq $count ]]; then
            is_last=true
        fi
        
        # Set the connector
        local connector="$TEE"
        local new_prefix="$prefix$PIPE"
        if $is_last; then
            connector="$ELBOW"
            new_prefix="$prefix$BLANK"
        fi
        
        # Check if item is directory or file
        if [[ -d "$item" ]]; then
            if $SHOW_DIRS; then
                echo -e "${prefix}${connector}${COLOR_DIR}${basename}/${COLOR_RESET}"
                DIR_COUNT=$((DIR_COUNT + 1))
                # Recursively process subdirectory
                print_tree "$item" "$new_prefix" $((depth + 1))
            fi
        elif [[ -f "$item" ]]; then
            if $SHOW_FILES; then
                echo -e "${prefix}${connector}${COLOR_FILE}${basename}${COLOR_RESET}"
                FILE_COUNT=$((FILE_COUNT + 1))
            fi
        fi
    done
}

# Parse command line arguments
while getopts ":dfL:h" opt; do
    case $opt in
        d)
            SHOW_FILES=false
            SHOW_DIRS=true
            ;;
        f)
            SHOW_FILES=true
            SHOW_DIRS=false
            ;;
        L)
            MAX_DEPTH=$OPTARG
            if ! [[ "$MAX_DEPTH" =~ ^[0-9]+$ ]]; then
                echo "Error: -L requires a numeric argument" >&2
                exit 1
            fi
            ;;
        h)
            usage
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            echo "Use -h for help" >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument" >&2
            exit 1
            ;;
    esac
done

# Shift past the options
shift $((OPTIND - 1))

# Get the directory to traverse
if [[ $# -gt 0 ]]; then
    START_DIR="$1"
fi

# Check if directory exists
if [[ ! -d "$START_DIR" ]]; then
    echo "Error: '$START_DIR' is not a valid directory" >&2
    exit 1
fi

# Print the root directory
echo -e "${COLOR_DIR}${START_DIR}${COLOR_RESET}"

# Start the tree traversal
print_tree "$START_DIR" "" 0

# Print summary
echo ""
if $SHOW_DIRS && $SHOW_FILES; then
    echo "$DIR_COUNT directories, $FILE_COUNT files"
elif $SHOW_DIRS; then
    echo "$DIR_COUNT directories"
elif $SHOW_FILES; then
    echo "$FILE_COUNT files"
fi
