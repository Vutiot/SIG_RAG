"""MCP server for web scraping utilities.

Provides tools via MCP protocol for sitemap exploration and link finding.
"""

from typing import Dict

from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp.types as types

from .tools import explore_sitemap, find_pdf_links, test_url, find_all_links


def create_server() -> Server:
    """Create and configure MCP server.

    Returns:
        Configured MCP server instance
    """
    server = Server("loire-bretagne-scraper")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        """List available tools."""
        return [
            types.Tool(
                name="explore_sitemap",
                description="Explore a sitemap XML file to find URLs. Can handle both sitemap indexes and regular sitemaps.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "URL to sitemap XML file"
                        },
                        "user_agent": {
                            "type": "string",
                            "description": "User agent string (optional)",
                            "default": "LB-RAG-Agent/1.0"
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "Request timeout in seconds (optional)",
                            "default": 30
                        }
                    },
                    "required": ["url"]
                }
            ),
            types.Tool(
                name="find_pdf_links",
                description="Find all PDF links on a web page. Scans <a> tags and other elements for PDF URLs.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "URL of the page to scan"
                        },
                        "user_agent": {
                            "type": "string",
                            "description": "User agent string (optional)",
                            "default": "LB-RAG-Agent/1.0"
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "Request timeout in seconds (optional)",
                            "default": 30
                        },
                        "include_metadata": {
                            "type": "boolean",
                            "description": "Include link text and metadata (optional)",
                            "default": True
                        }
                    },
                    "required": ["url"]
                }
            ),
            types.Tool(
                name="test_url",
                description="Test if a URL is accessible and get status information. Useful for checking URLs before scraping.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "URL to test"
                        },
                        "user_agent": {
                            "type": "string",
                            "description": "User agent string (optional)",
                            "default": "LB-RAG-Agent/1.0"
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "Request timeout in seconds (optional)",
                            "default": 30
                        },
                        "method": {
                            "type": "string",
                            "description": "HTTP method to use: HEAD or GET (optional)",
                            "default": "HEAD",
                            "enum": ["HEAD", "GET"]
                        }
                    },
                    "required": ["url"]
                }
            ),
            types.Tool(
                name="find_all_links",
                description="Find all links on a web page, optionally filtered by domain.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "URL of the page to scan"
                        },
                        "user_agent": {
                            "type": "string",
                            "description": "User agent string (optional)",
                            "default": "LB-RAG-Agent/1.0"
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "Request timeout in seconds (optional)",
                            "default": 30
                        },
                        "filter_domain": {
                            "type": "string",
                            "description": "Filter links to this domain (optional)"
                        }
                    },
                    "required": ["url"]
                }
            )
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: Dict) -> list[types.TextContent]:
        """Handle tool calls."""
        if name == "explore_sitemap":
            result = explore_sitemap(
                url=arguments["url"],
                user_agent=arguments.get("user_agent", "LB-RAG-Agent/1.0"),
                timeout=arguments.get("timeout", 30)
            )
        elif name == "find_pdf_links":
            result = find_pdf_links(
                url=arguments["url"],
                user_agent=arguments.get("user_agent", "LB-RAG-Agent/1.0"),
                timeout=arguments.get("timeout", 30),
                include_metadata=arguments.get("include_metadata", True)
            )
        elif name == "test_url":
            result = test_url(
                url=arguments["url"],
                user_agent=arguments.get("user_agent", "LB-RAG-Agent/1.0"),
                timeout=arguments.get("timeout", 30),
                method=arguments.get("method", "HEAD")
            )
        elif name == "find_all_links":
            result = find_all_links(
                url=arguments["url"],
                user_agent=arguments.get("user_agent", "LB-RAG-Agent/1.0"),
                timeout=arguments.get("timeout", 30),
                filter_domain=arguments.get("filter_domain")
            )
        else:
            raise ValueError(f"Unknown tool: {name}")

        import json
        return [types.TextContent(
            type="text",
            text=json.dumps(result, indent=2, ensure_ascii=False)
        )]

    return server


async def main():
    """Run the MCP server."""
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
