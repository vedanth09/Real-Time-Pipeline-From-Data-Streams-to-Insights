import csv

input_file = "/Users/vedanth/Desktop/projects/Movies API Pipeline/cleaned_movies06.csv"
output_file = "/Users/vedanth/Desktop/projects/Movies API Pipeline/cleaned_movies07.csv"
expected_columns = 16  # Set the expected number of columns

def clean_and_write_csv(input_file, output_file):
    """
    Cleans the input CSV by:
    - Removing rows with null values
    - Skipping rows with inconsistent column counts
    - Ensuring proper quoting of fields
    - Removing duplicate rows
    - Writing cleaned data to the output CSV
    """
    seen_rows = set()  # A set to track rows we've already seen
    
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)  # Ensure proper quoting

        line_number = 0
        for row in reader:
            line_number += 1
            try:
                if line_number <= 5:
                    print(f"Reading row {line_number}: {row}")

                # Check if row has the expected number of columns
                if len(row) != expected_columns:
                    print(f"Skipping row {line_number} due to incorrect number of columns. Found {len(row)} columns.")
                    continue  # Skip this row

                # Remove null values (empty strings or None)
                cleaned_row = [field for field in row if field not in ("", None)]

                # If there are remaining columns after cleaning, process the row
                if cleaned_row:
                    # Convert the row to a tuple (to be hashable) for checking duplicates
                    row_tuple = tuple(cleaned_row)
                    
                    # Check if the row is a duplicate
                    if row_tuple in seen_rows:
                        print(f"Skipping duplicate row {line_number}: {cleaned_row}")
                        continue  # Skip duplicate rows
                    
                    # Add the row to the set of seen rows
                    seen_rows.add(row_tuple)

                    # Ensure fields are properly quoted (for embedded commas inside quotes)
                    fixed_row = [f'"{field}"' if '"' in field else field for field in cleaned_row]
                    
                    # Debugging: Print the row being written
                    print(f"Writing row {line_number}: {fixed_row}")

                    writer.writerow(fixed_row)  # Write the row to the output file
                else:
                    print(f"Skipping empty row {line_number}")
            except Exception as e:
                print(f"Skipping line {line_number} due to error: {e}")

    print(f"Cleaned CSV saved to {output_file}")

clean_and_write_csv(input_file, output_file)
