import os

root = '/home/jon/workshop/real_chessism/last_chessism/'
root_files = ['main.py', 'Dockerfile','docker-compose.yml', 'constants.py','requirements.txt', 'testing_chessism.py']
service_folders = ['leela-service']
butler_folder = 'chessism_api'

intro = """
    PART ONE: ROOT FILES
            This is the chessism-api project, is a suite for chess analysis,
            it's source is mainly the chess.com players and games.
            
        """

part_two = """
            PART TWO: SERVICE
            This is the second part, it contains the service scripts for: 'leela-service'
            
            """

part_three = """
            PART THREE: CHESSISM_API ROUTERS
            This is the third part, it contains some general helpers for the api

            """

part_four = """
            PART FOUR: CHESSISM_API OPERATIONS
            This is the fourth part, it contains the 'operations' part of the chessism_api:
            
            """

part_five = """
            PART FIVE: CHESSISM_API DATABASE
            This is the fifth part, it contains the 'database' part of the chessism_api:
            
            """
prefixes = {0:intro,
           1:part_two,
           2:part_three,
           3:part_four,
           4:part_five}

def read_file_to_string(folder: str, filename:str) -> str:
    if folder == 'root':
        relative_path = root + filename
    else:
        relative_path = root + folder + '/' + filename
    with open(relative_path, 'r', encoding='utf-8') as file:
        string = file.read()
    return string

def write_txt_file(name:str, content: str):
    try:
        with open(name, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Successfully wrote content to {name}")

    except IOError as e:
        print(f"An error occurred while writing the file: {e}")
def create_root_files_for_llm():
    result = prefixes[0]
    result += "\n"
    for file in root_files:
        result += "\n\n"
        result += f"### {file} \n"
        result+= read_file_to_string(folder = 'root', filename = file)
        result += "\n\n"
    write_txt_file(name= "one.txt", content = result)
def create_service_files_for_llm():
    stop_words= ['.ipynb_checkpoints', '__pycache__']
    result = prefixes[1]
    result += "\n"
    for folder in service_folders:
        for file in os.listdir(root+folder):
             if file not in stop_words:
                 result += "\n\n"
                 result += f"### {folder}/{file} \n"
                 result += read_file_to_string(folder = folder, filename = file)
                 result += "\n\n"
    write_txt_file(name= "two.txt", content = result)
def create_routers_file_for_llm():
    stop_words= ['.ipynb_checkpoints', '__pycache__', '__init__.py']
    outside_routers_files = os.listdir(root+'/'+'chessism_api')
    outside_routers_files = [ x for x in outside_routers_files if x not in stop_words]
    outside_routers_files = [x for x in outside_routers_files if x.endswith('.py')]
    router_files = os.listdir(root + '/chessism_api/routers')
    router_files = [x for x in router_files if x not in stop_words]
    result = prefixes[2]
    result += "\n"
    for file in outside_routers_files:
        result += "\n\n"
        result += f"### inner_api/{file} \n"
        result+= read_file_to_string(folder = "chessism_api", filename = file)
        result += "\n\n"
    for file in router_files:
        result += "\n\n"
        result += f"### inner_api/routers/{file} \n"
        result+= read_file_to_string(folder = "chessism_api/routers", filename = file)
        result += "\n\n"
    write_txt_file(name= "three.txt", content = result)

def create_operations_file_for_llm():
    stop_words= ['.ipynb_checkpoints', '__pycache__', '__init__.py']
    oprations_files = os.listdir(root+'chessism_api/operations')
    oprations_files = [x for x in oprations_files if x not in stop_words]
    result = prefixes[3]
    result += "\n"
    for file in oprations_files:
        result += "\n\n"
        result += f"### inner_api/operations/{file} \n"
        result+= read_file_to_string(folder = "chessism_api/operations", filename = file)
        result += "\n\n"
    write_txt_file(name= "four.txt", content = result)
def create_database_file_for_llm():
    stop_words= ['.ipynb_checkpoints', '__pycache__', '__init__.py','sources']
    database_files = os.listdir(root+'chessism_api/database')
    database_files = [x for x in database_files if x not in stop_words]
    result = prefixes[4]
    result += "\n"
    for file in database_files:
        result += "\n\n"
        result += f"### inner_api/database/{file} \n"
        result+= read_file_to_string(folder = "chessism_api/database", filename = file)
        result += "\n\n"
    write_txt_file(name= "five.txt", content = result)