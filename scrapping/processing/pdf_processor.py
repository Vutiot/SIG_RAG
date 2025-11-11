"""PDF processing with OCR and text extraction.

Handles extraction of text from PDFs using:
- pdfplumber for native text extraction
- pytesseract for OCR when needed
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

import pdfplumber
import pytesseract
from pdf2image import convert_from_path
from PIL import Image
import structlog

from utils.state_manager import StateManager

logger = structlog.get_logger(__name__)


class PDFProcessor:
    """Process PDF files to extract text."""

    def __init__(
        self,
        task_id: str,
        state_manager: Optional[StateManager] = None,
        use_ocr: bool = True
    ):
        """Initialize PDF processor.

        Args:
            task_id: Task identifier
            state_manager: Optional state manager
            use_ocr: Whether to use OCR for scanned PDFs
        """
        self.task_id = task_id
        self.state_manager = state_manager
        self.use_ocr = use_ocr
        self.logger = logger.bind(task_id=task_id)

    def process_pdf(
        self,
        pdf_path: Path,
        output_path: Optional[Path] = None
    ) -> Dict:
        """Process a single PDF file.

        Args:
            pdf_path: Path to PDF file
            output_path: Optional output path for text (default: same name with .txt)

        Returns:
            Dict with extracted metadata and text
        """
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        # Check if already processed
        operation_key = str(pdf_path)
        if self.state_manager and self.state_manager.is_operation_completed(
            self.task_id, "pdf_process", operation_key
        ):
            self.logger.info("PDF already processed, skipping", pdf=str(pdf_path))
            if output_path and output_path.exists():
                with open(output_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                return {'text': text, 'pages': 0, 'source': str(pdf_path)}

        self.logger.info("Processing PDF", pdf=str(pdf_path))

        # Determine output path
        if output_path is None:
            output_path = pdf_path.with_suffix('.txt')

        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Try native text extraction first
            text, num_pages = self._extract_text_native(pdf_path)

            # If little/no text found and OCR enabled, try OCR
            if len(text.strip()) < 100 and self.use_ocr:
                self.logger.info("Little text found, trying OCR", pdf=str(pdf_path))
                text, num_pages = self._extract_text_ocr(pdf_path)

            # Save text to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(text)

            result = {
                'text': text,
                'pages': num_pages,
                'source': str(pdf_path),
                'output': str(output_path),
                'char_count': len(text),
                'word_count': len(text.split())
            }

            # Mark as completed
            if self.state_manager:
                self.state_manager.record_operation(
                    self.task_id,
                    "pdf_process",
                    operation_key,
                    metadata={'output': str(output_path), 'pages': num_pages}
                )

            self.logger.info(
                "PDF processed",
                pdf=str(pdf_path),
                pages=num_pages,
                chars=len(text),
                words=len(text.split())
            )

            return result

        except Exception as e:
            self.logger.error("PDF processing failed", pdf=str(pdf_path), error=str(e))
            raise

    def _extract_text_native(self, pdf_path: Path) -> tuple[str, int]:
        """Extract text using pdfplumber (native text).

        Args:
            pdf_path: Path to PDF

        Returns:
            Tuple of (text, num_pages)
        """
        text_parts = []
        num_pages = 0

        with pdfplumber.open(pdf_path) as pdf:
            num_pages = len(pdf.pages)

            for page_num, page in enumerate(pdf.pages, 1):
                page_text = page.extract_text()

                if page_text:
                    text_parts.append(f"--- Page {page_num} ---\n")
                    text_parts.append(page_text)
                    text_parts.append("\n\n")

        return ''.join(text_parts), num_pages

    def _extract_text_ocr(self, pdf_path: Path) -> tuple[str, int]:
        """Extract text using OCR (for scanned PDFs).

        Args:
            pdf_path: Path to PDF

        Returns:
            Tuple of (text, num_pages)
        """
        try:
            # Convert PDF to images
            images = convert_from_path(pdf_path)
            num_pages = len(images)

            text_parts = []

            for page_num, image in enumerate(images, 1):
                self.logger.debug(
                    "Running OCR on page",
                    page=page_num,
                    total=num_pages
                )

                # Run OCR
                page_text = pytesseract.image_to_string(
                    image,
                    lang='fra'  # French language
                )

                text_parts.append(f"--- Page {page_num} (OCR) ---\n")
                text_parts.append(page_text)
                text_parts.append("\n\n")

            return ''.join(text_parts), num_pages

        except Exception as e:
            self.logger.error("OCR failed", pdf=str(pdf_path), error=str(e))
            # Return empty text rather than failing completely
            return "", 0

    def process_directory(
        self,
        input_dir: Path,
        output_dir: Path,
        pattern: str = "*.pdf"
    ) -> List[Dict]:
        """Process all PDFs in a directory.

        Args:
            input_dir: Input directory with PDFs
            output_dir: Output directory for text files
            pattern: Glob pattern for PDF files

        Returns:
            List of result dicts
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        pdf_files = list(input_dir.rglob(pattern))
        self.logger.info(
            "Processing PDF directory",
            input_dir=str(input_dir),
            pdf_count=len(pdf_files)
        )

        results = []

        for pdf_path in pdf_files:
            try:
                # Maintain relative structure
                rel_path = pdf_path.relative_to(input_dir)
                output_path = output_dir / rel_path.with_suffix('.txt')

                result = self.process_pdf(pdf_path, output_path)
                results.append(result)

            except Exception as e:
                self.logger.error(
                    "Failed to process PDF",
                    pdf=str(pdf_path),
                    error=str(e)
                )
                results.append({
                    'source': str(pdf_path),
                    'error': str(e)
                })

        self.logger.info(
            "PDF directory processing complete",
            total=len(pdf_files),
            successful=len([r for r in results if 'error' not in r])
        )

        return results


def process_pdfs_batch(
    input_dir: Path,
    output_dir: Path,
    task_id: str,
    state_manager: Optional[StateManager] = None,
    use_ocr: bool = True
) -> List[Dict]:
    """Convenience function to process a batch of PDFs.

    Args:
        input_dir: Input directory
        output_dir: Output directory
        task_id: Task identifier
        state_manager: Optional state manager
        use_ocr: Whether to use OCR

    Returns:
        List of result dicts
    """
    processor = PDFProcessor(
        task_id=task_id,
        state_manager=state_manager,
        use_ocr=use_ocr
    )

    return processor.process_directory(input_dir, output_dir)


def extract_metadata_from_text(text: str) -> Dict:
    """Extract metadata from PDF text (title, date, etc.).

    Args:
        text: Extracted text

    Returns:
        Dict with extracted metadata
    """
    import re
    from datetime import datetime

    metadata = {}

    # Try to extract year
    years = re.findall(r'\b(20\d{2})\b', text[:1000])  # Look in first 1000 chars
    if years:
        metadata['year'] = int(years[0])

    # Try to extract month names (French)
    months_fr = {
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4,
        'mai': 5, 'juin': 6, 'juillet': 7, 'août': 8,
        'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
    }

    for month_name, month_num in months_fr.items():
        if month_name.lower() in text[:1000].lower():
            metadata['month'] = month_num
            metadata['month_name'] = month_name
            break

    # Try to extract title (usually in first few lines)
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if lines:
        # First substantial line is often the title
        for line in lines[:5]:
            if len(line) > 10 and len(line) < 200:
                metadata['title'] = line
                break

    return metadata
