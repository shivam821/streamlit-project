import os
import datetime
import mysql.connector  # Updated MySQL library
import streamlit as st
import shutil

# Function to create and return a log file name
def generate_log_file():
    try:
        log_directory = "Logs"
        os.makedirs(log_directory, exist_ok=True)
        str_file_name = datetime.datetime.now().strftime("%Y-%m-%d %H %M %S")
        log_file_name = os.path.join(log_directory, str_file_name + ".log")
        return log_file_name
    except Exception as ex:
        st.error("Error in creating log file: " + str(ex))
        return None

# Function to write messages to the log file
def write_log(log_file_name, msg):
    try:
        with open(log_file_name, 'a') as log_file:
            log_file.write(f"{datetime.datetime.now()} - {msg}\n")
    except Exception as ex:
        st.error("Error writing to log file: " + str(ex))

# Function to connect to MySQL database
def connect_to_mysql(ip, port, username, password, database):
    connection_string = {
        'host': ip,
        'port': int(port),
        'user': username,
        'password': password,
        'database': database
    }
    try:
        conn = mysql.connector.connect(**connection_string)
        st.success("Connection Successful")
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None

# Function to execute queries from files
def execute_queries(conn, query_folder_path, log_file_name):
    if not query_folder_path:
        st.error("Select Query folder")
        write_log(log_file_name, "Error: Query folder not selected")
        return

    write_log(log_file_name, "-------------------------------------------- EXECUTE Operation Started --------------------------------------------")
    write_log(log_file_name, f"Info: Query folder path {query_folder_path}")
    write_log(log_file_name, f"Info: Connection String = {conn.get_server_info()}")
    
    try:
        if os.path.exists(query_folder_path):
            for root, dirs, files in os.walk(query_folder_path):
                for file in files:
                    if file.endswith(".sql"):
                        with open(os.path.join(root, file), 'r') as f:
                            sql_script = f.read()
                            queries = [query.strip() for query in sql_script.split('GO') if query.strip()]
                            for query in queries:
                                try:
                                    cursor = conn.cursor()
                                    cursor.execute(query)
                                    conn.commit()
                                    write_log(log_file_name, "Success: Query executed successfully.")
                                except mysql.connector.Error as single_query_error:
                                    write_log(log_file_name, f"Error: EXECUTE: {single_query_error} : {query}")
                                    st.error("Error occurred while executing the queries, please refer to the log file")
                                    return
            st.success("Executed all queries")
        else:
            st.error("Query folder not present")
            write_log(log_file_name, "Error: Query folder not present")
    except Exception as ex:
        write_log(log_file_name, f"Error: {ex}")
    write_log(log_file_name, f"End Time : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    write_log(log_file_name, "-------------------------------------------- EXECUTE Operation Ended --------------------------------------------")

# Function to backup database
def backup_database(conn, backup_folder_path, log_file_name):
    if not backup_folder_path:
        st.error("Select Backup folder")
        write_log(log_file_name, "Error: Backup folder not selected")
        return

    dialog_result = st.radio("Do you want to take Backup?", ("Yes", "No"))
    if dialog_result == "Yes":
        write_log(log_file_name, "Info: Backup option selected")
        try:
            write_log(log_file_name, "Info: Backup Start")
            time_text = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            db_name = conn.database
            file_name = f"{db_name}_{time_text}.bak"
            backup_query = f"BACKUP DATABASE `{db_name}` TO DISK = '{os.path.join(backup_folder_path, file_name)}';"
            write_log(log_file_name, backup_query)
            cursor = conn.cursor()
            cursor.execute(backup_query)
            conn.commit()
            write_log(log_file_name, f"Success: Backup completed successfully")
            write_log(log_file_name, f"Success: Backup File Location: {os.path.join(backup_folder_path, file_name)}")
            st.success("Successfully Completed Backup")
        except mysql.connector.Error as ex:
            write_log(log_file_name, f"Error: Backup failed {ex}")
            st.error(f"Error: {ex}")

# Streamlit app
def main():
    st.title("DB Executor")

    log_file_name = generate_log_file()
    
    if not log_file_name:
        return

    ip = st.text_input("IP Address", "")
    port = st.text_input("Port", "")
    username = st.text_input("Username", "")
    password = st.text_input("Password", type="password")
    database = st.text_input("Database", "")

    query_folder_path = st.text_input("Query Folder Path", "")
    backup_folder_path = st.text_input("Backup Folder Path", "")

    if st.button("Connect"):
        conn = connect_to_mysql(ip, port, username, password, database)
        if conn:
            st.session_state.conn = conn

    if st.button("Execute Queries"):
        if 'conn' in st.session_state:
            execute_queries(st.session_state.conn, query_folder_path, log_file_name)
        else:
            st.error("Please connect to the database first.")

    if st.button("Backup Database"):
        if 'conn' in st.session_state:
            backup_database(st.session_state.conn, backup_folder_path, log_file_name)
        else:
            st.error("Please connect to the database first.")

if __name__ == "__main__":
    main()
