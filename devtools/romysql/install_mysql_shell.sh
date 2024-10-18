#!/bin/bash

# Detect system architecture (x86_64, arm64, etc.)
ARCH=$(uname -m)

# Detect OS platform (Linux or Darwin for macOS)
OS=$(uname -s)

# Function to install MySQL Shell on Linux
install_mysql_shell_linux() {
    echo "Installing MySQL Shell on Linux..."

    # Set the base URL for MySQL Shell downloads
    BASE_URL="https://dev.mysql.com/get/Downloads/MySQL-Shell"

    # Dynamically fetch the latest MySQL Shell version
    LATEST_VERSION=$(curl -s https://dev.mysql.com/downloads/shell/ | grep -oP '(?<=MySQL Shell )\d+\.\d+\.\d+' | head -n 1)

    # Determine architecture for RPM download
    if [[ "$ARCH" == "x86_64" ]]; then
        PACKAGE_NAME="mysql-shell-${LATEST_VERSION}-1.el9.x86_64.rpm"
    elif [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
        PACKAGE_NAME="mysql-shell-${LATEST_VERSION}-1.el9.aarch64.rpm"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi

    # Download the RPM package
    DOWNLOAD_URL="${BASE_URL}/${PACKAGE_NAME}"
    echo "Downloading MySQL Shell version ${LATEST_VERSION} from $DOWNLOAD_URL..."
    curl -O "$DOWNLOAD_URL"

    # Install the package
    echo "Installing MySQL Shell..."
    sudo dnf install -y ./$PACKAGE_NAME

    # Cleanup the RPM package after installation
    rm -f ./$PACKAGE_NAME
}

# Function to install MySQL Shell on macOS
install_mysql_shell_macos() {
    echo "Installing MySQL Shell on macOS..."

    # macOS-specific installation method (Homebrew)
    if [[ "$ARCH" == "x86_64" || "$ARCH" == "arm64" ]]; then
        # Check if Homebrew is installed
        if ! command -v brew &> /dev/null; then
            echo "Homebrew not found. Installing Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        fi

        # Install MySQL Shell using Homebrew
        echo "Installing MySQL Shell using Homebrew..."
        brew install mysql-shell
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi
}

# Main function
main() {
    # Determine the platform and install MySQL Shell accordingly
    if [[ "$OS" == "Linux" ]]; then
        install_mysql_shell_linux
    elif [[ "$OS" == "Darwin" ]]; then
        install_mysql_shell_macos
    else
        echo "Unsupported operating system: $OS"
        exit 1
    fi

    # Verify installation
    mysqlsh --version
}

# Run the main function
main