import os
import datetime
import mysql.connector
import logging
import streamlit as st

def setup_logging():
    log_directory = os.path.join(os.path.dirname(__file__), "Logs")
    os.makedirs(log_directory, exist_ok=True)
    log_file_name = datetime.datetime.now().strftime("%Y-%m-%d %H %M %S") + ".log"
    log_file_path = os.path.join(log_directory, log_file_name)
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(message)s')
    return log_file_path

def write_log(msg):
    logging.info(msg)

def connect_to_mysql(ip, port, username, password, database):
    connection_string = {
        'host': ip,
        'port': port,
        'user': username,
        'password': password,
        'database': database
    }
    try:
        conn = mysql.connector.connect(**connection_string)
        write_log("Connection Successful to Host : " + str(connection_string['host']))
        st.success("Successfully connected to Database Server")
        return conn
    except mysql.connector.Error as err:
        write_log("Error: Connection Failed - " + str(err))
        st.error(f"Failed to connect to database: {str(err)}")
        return None

def execute_queries(conn, query_folder_path):
    if not query_folder_path:
        st.error("Select Query folder")
        write_log("Error: Query folder not selected")
        return

    write_log("-------------------------------------------- EXECUTE Operation Started --------------------------------------------")
    write_log("Info: Query folder path " + query_folder_path)
    
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
                                    write_log("Success: Query executed successfully.")
                                    cursor.close()
                                except mysql.connector.Error as single_query_error:
                                    write_log("Error: EXECUTE: " + str(single_query_error) + " : " + query)
                                    st.error("Error occurred while executing the queries, please refer to the log file")
                                    return
            st.success("Executed all queries")
        else:
            st.error("Query folder not present")
            write_log("Error: Query folder not present")
    except Exception as ex:
        write_log("Error: " + str(ex))
    write_log("End Time : " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    write_log("-------------------------------------------- EXECUTE Operation Ended --------------------------------------------")

def backup_database(conn, backup_folder_path):
    if not backup_folder_path:
        st.error("Select Backup folder")
        write_log("Error: Backup folder not selected")
        return

    if st.button("Do you want to take Backup?"):
        write_log("Info: Backup option selected")
        try:
            write_log("Info: Backup Start")
            time_text = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            db_name = conn.database
            file_name = f"{db_name}_{time_text}.sql"
            backup_query = f"BACKUP DATABASE `{db_name}` TO DISK = '{os.path.join(backup_folder_path, file_name)}';"
            write_log(backup_query)
            cursor = conn.cursor()
            cursor.execute(backup_query)
            conn.commit()
            cursor.close()
            write_log("Success: Backup completed successfully")
            write_log("Success: Backup File Location: " + os.path.join(backup_folder_path, file_name))
            st.success("Successfully Completed Backup")
        except mysql.connector.Error as ex:
            write_log("Error: Backup failed " + str(ex))
            st.error("Error: " + str(ex))

def main():
    st.title("DB Executor")
    
    setup_logging()
    
    conn = None  # Initialize conn to None

    ip = st.text_input("IP Address")
    port = st.text_input("Port", value="3306")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    database = st.text_input("Database")
    
    query_folder_path = st.text_input("Query Folder Path")
    backup_folder_path = st.text_input("Backup Folder Path")
    
    if st.button("Connect"):
        conn = connect_to_mysql(ip, port, username, password, database)
    
    if conn:
        if st.button("Execute Queries"):
            execute_queries(conn, query_folder_path)
        
        if st.button("Backup Database"):
            backup_database(conn, backup_folder_path)

if __name__ == "__main__":
    main()
