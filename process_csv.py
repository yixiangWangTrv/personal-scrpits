import csv
import os
import sys
from pathlib import Path

class CSVProcessor:
    def __init__(self, filename):
        self.downloads_path = str(Path.home() / "Documents")
        self.file_path = os.path.join(self.downloads_path, filename)
        self.results = []
        self.stats = {
            'total_rows': 0,
            'processed': 0,
            'skipped': 0,
            'errors': 0
        }
    
    def process_amount(self, amount_str):
        """Process amount string: remove commas only, do NOT multiply by 1000"""
        if not amount_str:
            return None
        
        # Remove all non-numeric characters except dot and minus sign
        cleaned = amount_str.replace(',', '').replace(' ', '')
        
        # Check if valid number
        try:
            # Handle possible negative numbers in parentheses
            if cleaned.startswith('(') and cleaned.endswith(')'):
                cleaned = '-' + cleaned[1:-1]
            
            # Convert to number (float or int)
            try:
                # Try to convert to integer first
                return int(cleaned)
            except ValueError:
                # If not integer, convert to float
                amount = float(cleaned)
                # Keep as float, don't multiply by 1000
                return amount
        except (ValueError, AttributeError) as e:
            print(f"Amount conversion error '{amount_str}': {e}")
            return None
    
    def process_file(self):
        """Process CSV file"""
        if not os.path.exists(self.file_path):
            print(f"❌ File not found: {self.file_path}")
            return False
        
        print(f"📁 Processing file: {self.file_path}")
        print(f"⏳ Reading data...")
        
        # Try multiple encodings
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(self.file_path, 'r', encoding=encoding) as file:
                    reader = csv.reader(file)
                    
                    for row_index, row in enumerate(reader):
                        self.stats['total_rows'] += 1
                        
                        # Skip first 4 rows
                        if row_index < 4:
                            continue
                        
                        # Check if row has enough columns
                        if len(row) < 9:
                            self.stats['skipped'] += 1
                            continue
                        
                        hotel_id = row[3].strip() if len(row) > 3 else ''
                        currency = row[7].strip() if len(row) > 7 else 'IDR'  # Default
                        amount_str = row[8].strip() if len(row) > 8 else ''
                        
                        # Skip empty rows
                        if not hotel_id or not amount_str:
                            self.stats['skipped'] += 1
                            continue
                        
                        # Process amount
                        amount = self.process_amount(amount_str)
                        if amount is None:
                            self.stats['errors'] += 1
                            continue
                        
                        # Format output (with newline at the end)
                        formatted = f'{{hotelId: "{hotel_id}", currency: "{currency}", amount: {amount}}},\n'
                        self.results.append(formatted)
                        self.stats['processed'] += 1
                
                # If successfully read, break the loop
                print(f"✅ Successfully read file using {encoding} encoding")
                break
                
            except UnicodeDecodeError:
                print(f"⚠️  Encoding {encoding} failed, trying next...")
                continue
            except Exception as e:
                print(f"❌ Error processing file: {e}")
                return False
        
        return True
    
    def export_results(self):
        """Export results"""
        if not self.results:
            print("⚠️  No data to export")
            return
        
        # Generate output string - join all results (each already has newline)
        output = ''.join(self.results)
        
        # Remove trailing comma and newline from the last item for cleaner output
        if output.endswith(',\n'):
            output = output[:-2]  # Remove the last comma and newline
        
        # Print statistics
        print("\n" + "="*60)
        print("📊 Processing Statistics:")
        print(f"   Total rows: {self.stats['total_rows']}")
        print(f"   Successfully processed: {self.stats['processed']}")
        print(f"   Skipped rows: {self.stats['skipped']}")
        print(f"   Error rows: {self.stats['errors']}")
        print("="*60)
        
        # Export to files
        timestamp = os.path.getmtime(self.file_path)
        from datetime import datetime
        timestamp_str = datetime.fromtimestamp(timestamp).strftime('%Y%m%d_%H%M%S')
        
        output_files = []
        
        # 1. Text format with each item on new line
        txt_filename = f"export_{timestamp_str}.txt"
        txt_path = os.path.join(self.downloads_path, txt_filename)
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(output)
        output_files.append(txt_path)
        
        # 2. JSON array format with each item on new line
        # Create JSON array with each element on new line
        json_items = [item.rstrip(',\n') for item in self.results]
        json_output = '[\n' + ',\n'.join(json_items) + '\n]'
        json_filename = f"export_{timestamp_str}.json"
        json_path = os.path.join(self.downloads_path, json_filename)
        with open(json_path, 'w', encoding='utf-8') as f:
            f.write(json_output)
        output_files.append(json_path)
        
        # 3. One per line format (already formatted)
        lines_output = ''.join(self.results)
        lines_filename = f"export_{timestamp_str}_lines.txt"
        lines_path = os.path.join(self.downloads_path, lines_filename)
        with open(lines_path, 'w', encoding='utf-8') as f:
            f.write(lines_output)
        output_files.append(lines_path)
        
        # Show preview of results
        print("\n📋 Results Preview (first 5 lines):")
        print("-" * 60)
        for i, result in enumerate(self.results[:5]):
            result = result.rstrip('\n')  # Remove newline for display
            print(f"{i+1}. {result}")
        
        if len(self.results) > 5:
            print(f"... plus {len(self.results) - 5} more rows")
        
        print("\n💾 Exported files:")
        for file_path in output_files:
            print(f"   📄 {os.path.basename(file_path)}")
        
        print("\n✅ Processing completed!")
        return output_files

def main():
    # Configuration
    filename = "TERA Deposit Write Off 2025 - Monitoring - deposit to write-off 2.csv"
    
    # Create processor
    processor = CSVProcessor(filename)
    
    # Process file
    if processor.process_file():
        # Export results
        processor.export_results()
        
        # Ask if user wants to see full results
        if processor.results:
            response = input("\nShow full results? (y/n): ").lower()
            if response == 'y':
                print("\n" + "="*80)
                # Remove trailing comma from the last item for display
                results_display = ''.join(processor.results)
                if results_display.endswith(',\n'):
                    results_display = results_display[:-2]
                print(results_display)
                print("="*80)
    else:
        print("❌ Failed to process file")

if __name__ == "__main__":
    main()