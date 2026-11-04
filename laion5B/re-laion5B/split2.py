import pyarrow.parquet as pq
import pyarrow as pa
import os

def split_parquet_file(input_file, output_dir, batch_size):
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Read the Parquet file into a PyArrow Table
    print(f"Reading Parquet file: {input_file}")
    table = pq.read_table(input_file)

    # Total number of rows in the table
    num_rows = table.num_rows
    print(f"Total number of rows: {num_rows}")

    # Calculate the number of files needed
    num_files = (num_rows // batch_size) + 1

    # Split and write the table into smaller Parquet files
    for i in range(num_files):
        start_row = i * batch_size
        end_row = min((i + 1) * batch_size, num_rows)
        
        # Slice the table
        batch_table = table.slice(start_row, end_row - start_row)
        
        # Define the output file path
        output_file = os.path.join(output_dir, f"part_{i}.parquet")
        
        # Write the sliced table to a new Parquet file
        print(f"Writing {output_file} with rows {start_row} to {end_row - 1}")
        pq.write_table(batch_table, output_file, compression='snappy')
    
    print(f"Completed splitting {input_file}. {num_files} files written to {output_dir}")

def process_directory(input_dir, output_base_dir, batch_size):
    # Loop through all Parquet files in the input directory
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".parquet"):
                input_file = os.path.join(root, file)
                relative_path = os.path.relpath(root, input_dir)
                output_dir = os.path.join(output_base_dir, relative_path, os.path.splitext(file)[0])

                print(f"Processing file: {input_file}")
                
                # Split the Parquet file
                split_parquet_file(input_file, output_dir, batch_size)

if __name__ == "__main__":
    input_dir = "/media/hd2/backup_30802/aesthetics_v2_4.75"
    output_base_dir = "/media/hd2/backup_30802/aesthetics_v2_4.75_fixed"

    # Number of rows per output file
    batch_size = 100000  # Adjust this value as needed
    
    # Process all files in the input directory
    process_directory(input_dir, output_base_dir, batch_size)

