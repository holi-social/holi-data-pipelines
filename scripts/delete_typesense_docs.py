#!/usr/bin/env python3
# mypy: ignore-errors
# ruff: noqa

"""
Script to delete documents from a Typesense collection.
Usage: python delete_documents.py <collection_name> <doc_id1> <doc_id2> ...
"""

import os
import sys
import requests
import json
from typing import List, Dict, Any
import argparse


class TypesenseDeleter:
    def __init__(self, api_key: str, host: str = "localhost", port: int = 8108, protocol: str = "http"):
        """
        Initialize the Typesense deleter.

        Args:
            api_key: Typesense admin API key
            host: Typesense server host (default: localhost)
            port: Typesense server port (default: 8108)
            protocol: Protocol to use (default: http)
        """
        self.api_key = api_key
        self.base_url = f"{protocol}://{host}:{port}"
        self.headers = {"X-TYPESENSE-API-KEY": self.api_key, "Content-Type": "application/json"}

    def verify_connection(self) -> bool:
        """
        Verify connection to Typesense server.

        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            response = requests.get(f"{self.base_url}/health", headers=self.headers, timeout=10)
            return response.status_code == 200
        except requests.RequestException as e:
            print(f"Error connecting to Typesense server: {e}")
            return False

    def collection_exists(self, collection_name: str) -> bool:
        """
        Check if a collection exists.

        Args:
            collection_name: Name of the collection

        Returns:
            bool: True if collection exists, False otherwise
        """
        try:
            response = requests.get(f"{self.base_url}/collections/{collection_name}", headers=self.headers, timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def delete_document(self, collection_name: str, doc_id: str) -> Dict[str, Any]:
        """
        Delete a single document from the collection.

        Args:
            collection_name: Name of the collection
            doc_id: ID of the document to delete

        Returns:
            dict: Response from Typesense API
        """
        try:
            response = requests.delete(
                f"{self.base_url}/collections/{collection_name}/documents/{doc_id}", headers=self.headers, timeout=30
            )

            if response.status_code == 200:
                return {"success": True, "id": doc_id, "message": "Document deleted successfully"}
            elif response.status_code == 404:
                return {"success": False, "id": doc_id, "message": "Document not found"}
            else:
                return {"success": False, "id": doc_id, "message": f"Error {response.status_code}: {response.text}"}
        except requests.RequestException as e:
            return {"success": False, "id": doc_id, "message": f"Request failed: {str(e)}"}

    def delete_documents_batch(self, collection_name: str, doc_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Delete multiple documents from the collection.

        Args:
            collection_name: Name of the collection
            doc_ids: List of document IDs to delete

        Returns:
            list: List of results for each deletion attempt
        """
        results = []
        total_docs = len(doc_ids)

        print(f"Deleting {total_docs} documents from collection '{collection_name}'...")

        for i, doc_id in enumerate(doc_ids, 1):
            print(f"Progress: {i}/{total_docs} - Deleting document ID: {doc_id}")
            result = self.delete_document(collection_name, doc_id)
            results.append(result)

            if result["success"]:
                print(f"  ✓ Success: {result['message']}")
            else:
                print(f"  ✗ Failed: {result['message']}")

        return results


def parse_json_env_var(env_var_name: str, default_value: Any = None) -> Any:
    """
    Parse environment variable as JSON value.

    Args:
        env_var_name: Name of the environment variable
        default_value: Default value if env var is not set

    Returns:
        Parsed JSON value or default

    Raises:
        SystemExit: If JSON parsing fails
    """
    env_value = os.getenv(env_var_name)
    if env_value is None:
        return default_value

    try:
        return json.loads(env_value)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in environment variable {env_var_name}: {env_value}")
        print(f"JSON parsing error: {e}")
        print(f"Expected format for strings: '{env_var_name}=\"hostname\"'")
        print(f"Expected format for numbers: '{env_var_name}=8108'")
        sys.exit(1)


def get_env_config() -> Dict[str, Any]:
    """
    Get configuration from environment variables (parsed as JSON).

    Returns:
        dict: Configuration dictionary with defaults

    Raises:
        SystemExit: If required environment variables are not set or invalid JSON
    """
    # Required environment variable (parsed as JSON)
    api_key = parse_json_env_var("TYPESENSE_ADMIN_API_KEY")
    if not api_key:
        print("Error: TYPESENSE_ADMIN_API_KEY environment variable is not set.")
        print("Please set it with your Typesense admin API key as JSON:")
        print("export TYPESENSE_ADMIN_API_KEY='\"your_api_key_here\"'")
        sys.exit(1)

    # Optional environment variables with defaults (parsed as JSON)
    config = {
        "api_key": api_key,
        "host": parse_json_env_var("TYPESENSE_HOST", "localhost"),
        "port": parse_json_env_var("TYPESENSE_PORT", 8108),
        "protocol": parse_json_env_var("TYPESENSE_PROTOCOL", "http"),
        "collection_name": parse_json_env_var("TYPESENSE_COLLECTION_NAME"),
    }

    # Validate types
    if not isinstance(config["api_key"], str):
        print(f"Error: TYPESENSE_ADMIN_API_KEY must be a string. Got: {type(config['api_key']).__name__}")
        sys.exit(1)

    if not isinstance(config["host"], str):
        print(f"Error: TYPESENSE_HOST must be a string. Got: {type(config['host']).__name__}")
        sys.exit(1)

    if not isinstance(config["port"], int):
        print(f"Error: TYPESENSE_PORT must be a number. Got: {type(config['port']).__name__}")
        sys.exit(1)

    if not isinstance(config["protocol"], str):
        print(f"Error: TYPESENSE_PROTOCOL must be a string. Got: {type(config['protocol']).__name__}")
        sys.exit(1)

    if config["collection_name"] is not None and not isinstance(config["collection_name"], str):
        print(f"Error: TYPESENSE_COLLECTION_NAME must be a string. Got: {type(config['collection_name']).__name__}")
        sys.exit(1)

    # Validate protocol
    if config["protocol"] not in ["http", "https"]:
        print(f"Error: Invalid TYPESENSE_PROTOCOL '{config['protocol']}'. Must be 'http' or 'https'.")
        sys.exit(1)

    return config


def main():
    """Main function to handle command line arguments and execute deletion."""
    # Get environment configuration first
    env_config = get_env_config()

    parser = argparse.ArgumentParser(
        description="Delete documents from a Typesense collection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Environment Variables (JSON format - with current values):
  TYPESENSE_ADMIN_API_KEY: {"✓ Set" if env_config["api_key"] else "✗ Not set"}
  TYPESENSE_HOST: {json.dumps(env_config["host"])}
  TYPESENSE_PORT: {env_config["port"]}
  TYPESENSE_PROTOCOL: {json.dumps(env_config["protocol"])}
  TYPESENSE_COLLECTION_NAME: {json.dumps(env_config["collection_name"]) if env_config["collection_name"] else "Not set"}

Environment Variable Examples:
  export TYPESENSE_ADMIN_API_KEY='"your_api_key_here"'
  export TYPESENSE_HOST='"typesense.example.com"'
  export TYPESENSE_PORT=443
  export TYPESENSE_PROTOCOL='"https"'
  export TYPESENSE_COLLECTION_NAME='"my_collection"'

Usage Examples:
  # Using environment variables
  python delete_documents.py doc1 doc2 doc3

  # Override collection name
  python delete_documents.py my_collection doc1 doc2 doc3

  # Override server settings
  python delete_documents.py --host typesense.example.com --port 443 --protocol https my_collection doc1 doc2
        """,
    )

    # Collection name is optional if set in environment
    if env_config["collection_name"]:
        parser.add_argument(
            "collection_name",
            nargs="?",
            default=env_config["collection_name"],
            help=f"Name of the Typesense collection (default from env: {env_config['collection_name']})",
        )
    else:
        parser.add_argument("collection_name", help="Name of the Typesense collection")

    parser.add_argument("doc_ids", nargs="+", help="Document IDs to delete")
    parser.add_argument(
        "--host", default=env_config["host"], help=f"Typesense server host (default from env: {env_config['host']})"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=env_config["port"],
        help=f"Typesense server port (default from env: {env_config['port']})",
    )
    parser.add_argument(
        "--protocol",
        choices=["http", "https"],
        default=env_config["protocol"],
        help=f"Protocol to use (default from env: {env_config['protocol']})",
    )
    parser.add_argument("--confirm", action="store_true", help="Skip confirmation prompt")

    args = parser.parse_args()

    # Use API key from environment
    api_key = env_config["api_key"]

    # Show configuration being used
    print("Configuration:")
    print(f"  Host: {args.host}")
    print(f"  Port: {args.port}")
    print(f"  Protocol: {args.protocol}")
    print(f"  Collection: {args.collection_name}")
    print(f"  API Key: {'*' * (len(api_key) - 4) + api_key[-4:] if len(api_key) > 4 else '****'}")
    print()

    # Initialize deleter
    deleter = TypesenseDeleter(api_key=api_key, host=args.host, port=args.port, protocol=args.protocol)

    # Verify connection
    print("Verifying connection to Typesense server...")
    if not deleter.verify_connection():
        print("Failed to connect to Typesense server. Please check your configuration.")
        sys.exit(1)
    print("✓ Connection successful")

    # Verify collection exists
    print(f"Checking if collection '{args.collection_name}' exists...")
    if not deleter.collection_exists(args.collection_name):
        print(f"Error: Collection '{args.collection_name}' does not exist.")
        sys.exit(1)
    print("✓ Collection found")

    # Confirmation prompt
    if not args.confirm:
        print(f"\nYou are about to delete {len(args.doc_ids)} documents from collection '{args.collection_name}':")
        for doc_id in args.doc_ids[:10]:  # Show first 10 IDs
            print(f"  - {doc_id}")
        if len(args.doc_ids) > 10:
            print(f"  ... and {len(args.doc_ids) - 10} more documents")

        confirm = input("\nDo you want to proceed? (y/N): ").strip().lower()
        if confirm not in ["y", "yes"]:
            print("Operation cancelled.")
            sys.exit(0)

    # Delete documents
    print("\nStarting deletion process...")
    results = deleter.delete_documents_batch(args.collection_name, args.doc_ids)

    # Summary
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    print(f"\n{'=' * 50}")
    print("DELETION SUMMARY")
    print(f"{'=' * 50}")
    print(f"Total documents processed: {len(results)}")
    print(f"Successfully deleted: {successful}")
    print(f"Failed to delete: {failed}")

    if failed > 0:
        print("\nFailed deletions:")
        for result in results:
            if not result["success"]:
                print(f"  - ID: {result['id']} - {result['message']}")
        sys.exit(1)
    else:
        print("\n✓ All documents deleted successfully!")


if __name__ == "__main__":
    main()
