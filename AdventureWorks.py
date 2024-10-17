import sys
import os
import json
import pyodbc
from dotenv import load_dotenv
from openai import OpenAI

# Function to establish a connection with the SQL Server database
load_dotenv()

def connect_to_database():
    print("Attempting to connect to the database...")  # Print before the connection attempt
    server_name = 'localhost'
    database_name = 'AdventureWorks2019'
    db_username = os.getenv("DB_USERNAME")  # Load username from environment variable
    db_password = os.getenv("DB_PASSWORD")  # Load password from environment variable

    try:
        # Print to indicate the connection is starting
        print("Creating connection object...")

        # Create a connection object using ODBC
        connection = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};'
            f'DATABASE={database_name};UID={db_username};PWD={db_password}',
            timeout=5  # Set a timeout for the connection attempt
        )

        # If successful, print this message
        print("Connection to the database was successful.")
        return connection
    except pyodbc.Error as e:
        # If a pyodbc error occurs, print the error
        print("Error while connecting to the database (pyodbc error):", e)
        return None
    except Exception as e:
        # For any other exceptions, print the error
        print("An unexpected error occurred:", e)
        return None

# Function to fetch the structure of the database (schemas, tables, and columns)
def fetch_database_structure(connection):
    structure_info = {}
    query = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME;
    """
    # Execute the query to get information about all schemas, tables, and columns
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        schema_table_key = f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
        if schema_table_key not in structure_info:
            structure_info[schema_table_key] = []
        structure_info[schema_table_key].append(row.COLUMN_NAME)
    print(structure_info)
    return structure_info

# Define the directory and file path for logging
LOG_DIRECTORY = r"C:\Users\srouaud\PycharmProjects\OpenAI\Log files"
os.makedirs(LOG_DIRECTORY, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOG_DIRECTORY, "WelcomeWords151024.txt")

# Load environment variables (such as API keys for OpenAI)

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Ask the user if they want to keep the history
load_history = input("Do you want to read the conversation history from the log file? (Y/N): ").strip().upper()

# Redirect sys.stdout to a log file to save all outputs
sys.stdout = open(LOG_FILE_PATH, 'a')

# Function to load conversation history from the log file
def load_conversation_history(log_file):
    messages = []
    if os.path.exists(log_file):
        with open(log_file, 'r') as file:
            for line in file:
                try:
                    message = json.loads(line.strip())
                    messages.append(message)
                except json.JSONDecodeError:
                    continue
    return messages

# Function to log messages to the log file
def log_message(message, log_file):
    with open(log_file, 'a') as file:
        file.write(json.dumps(message) + "\n")

if load_history == "Y":
    conversation_history = load_conversation_history(LOG_FILE_PATH)
else:
    conversation_history = []

# Connect to the database
db_connection = connect_to_database()
# Fetch the database structure
database_structure = fetch_database_structure(db_connection)

# Create a message about the database structure and update the conversation history
# Now including schema information
db_structure_context = (
    f"The database structure consists of the following schemas, tables, and columns: "
    f"{json.dumps(database_structure)}. "
    "Note: Tables are accessible by querying SCHEMA_NAME.TABLE_NAME."
)
structure_message = {
    "role": "system",
    "content": db_structure_context
}
conversation_history.append(structure_message)

# Update the SQL generation prompt with schema information and accessibility details
sql_generation_prompt = {
    "role": "user",
    "content": (
        "Based only on the schema, table, and column names from the database provided, "
        "generate only a raw SQL query without any explanations. "
        "The tables are accessible by querying SCHEMA_NAME.TABLE_NAME. "
        "Use SQL Server Statement only"
        "I am using SQL server so you need to avoid exceeding the 8000-byte limit. "
        "The query should answer the question: 'Quel est l'age moyen des employés??'"

    )
}
conversation_history.append(sql_generation_prompt)

# Generate SQL query using OpenAI with current conversation history
chat_completion = openai_client.chat.completions.create(
    messages=conversation_history,
    model="gpt-4-turbo",
)

# Extract AI-generated message and log it
ai_message = {
    "role": "assistant",
    "content": chat_completion.choices[0].message.content.strip()  # Stripping any leading/trailing spaces
}
log_message(structure_message, LOG_FILE_PATH)
log_message(ai_message, LOG_FILE_PATH)

# Clean the AI-generated query by removing any code block markers
query = ai_message["content"].replace("```sql", "").replace("```", "").strip()

try:
    cursor = db_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    formatted_results = ""
    if results:
        column_names = [column[0] for column in cursor.description]
        for row in results:
            row_data = ", ".join(f"{column_names[i]}: {value}" for i, value in enumerate(row))
            formatted_results += row_data + "\n"
    else:
        formatted_results = "No results found for the query."

    # Generate commentary on the query results using OpenAI
    commentary_prompt = (
        f"Given the following SQL query results, please generate a descriptive commentary:\n\n{formatted_results}.")

    commentary_message = {"role": "user", "content": commentary_prompt}
    conversation_history.append(commentary_message)

    commentary_completion = openai_client.chat.completions.create(
        messages=conversation_history,
        model="gpt-4-turbo",
    )
    commentary_ai_message = {
        "role": "assistant",
        "content": commentary_completion.choices[0].message.content,
    }
    log_message({"role": "assistant", "content": formatted_results}, LOG_FILE_PATH)
    log_message(commentary_ai_message, LOG_FILE_PATH)
    print("Commentary on Query Results:\n" + commentary_ai_message["content"])
except Exception as e:
    # Handle and log any exceptions
    error_message = f"An error occurred: {str(e)}"
    print(error_message)
    log_message({"role": "assistant", "content": error_message}, LOG_FILE_PATH)
finally:
    # Ensure the database connection is closed
    if db_connection is not None:
        db_connection.close()

# Display generated SQL query to user
print("\nGenerated SQL Query:")
print(query)

# Close the log file to ensure all data is written
sys.stdout.close()
