import pandas as pd

# Load the CSV file
try:
    df = pd.read_csv('data-2.csv')
    print("CSV file loaded successfully.")
except Exception as e:
    print(f"Error loading CSV file: {e}")

# List to hold Cypher queries
cypher_queries = []

print("Starting to process CSV rows...")
# Iterate over the rows in the DataFrame
for index, row in df.iterrows():
    try:
        # Log processing of the current row
        print(f"Processing row {index + 1}...")

        # Create nodes
        customer_query = f"MERGE (c:Customer {{CustomerID: '{row['CustomerID']}', Country: '{row['Country']}'}})"
        product_query = f"MERGE (p:Product {{StockCode: '{row['StockCode']}', Description: '{row['Description']}', UnitPrice: {row['UnitPrice']}}})"
        transaction_query = f"MERGE (t:Transaction {{InvoiceNo: '{row['InvoiceNo']}', InvoiceDate: '{row['InvoiceDate']}', Quantity: {row['Quantity']}}})"

        # Create relationships
        customer_transaction_query = f"MERGE (c)-[:MADE]->(t)"
        transaction_product_query = f"MERGE (t)-[:INCLUDES]->(p)"

        # Add queries to the list
        cypher_queries.append(customer_query)
        cypher_queries.append(product_query)
        cypher_queries.append(transaction_query)
        cypher_queries.append(customer_transaction_query)
        cypher_queries.append(transaction_product_query)
    except Exception as e:
        print(f"Error processing row {index}: {e}")

print("Finished processing rows. Writing to file...")

# Write the Cypher queries to a file
try:
    with open('queries.cypher', 'w') as f:
        for query in cypher_queries:
            f.write(query + ';\n')
    print("Cypher queries have been generated and saved to queries.cypher")
except Exception as e:
    print(f"Error writing to file: {e}")


# Function to split the file into two parts
def split_file(input_file, output_file_1, output_file_2):
    try:
        with open(input_file, 'r') as f:
            lines = f.readlines()

        # Calculate the midpoint
        midpoint = len(lines) // 2

        # Write the first half to output_file_1
        with open(output_file_1, 'w') as f1:
            f1.writelines(lines[:midpoint])

        # Write the second half to output_file_2
        with open(output_file_2, 'w') as f2:
            f2.writelines(lines[midpoint:])

        print(f"File has been split into {output_file_1} and {output_file_2}.")
    except Exception as e:
        print(f"Error splitting file: {e}")


# Define file names for splitting
input_file = 'queries.cypher'
output_file_1 = 'queries_part1.cypher'
output_file_2 = 'queries_part2.cypher'

# Split the file
split_file(input_file, output_file_1, output_file_2)
