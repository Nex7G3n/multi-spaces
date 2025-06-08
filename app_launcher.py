import os

clidriver_path = r"C:\Users\lrosa\Desktop\UNT\C07\ing-software\apps\multi-spaces\venv311\Lib\site-packages\clidriver\bin"

if os.path.exists(clidriver_path):
    os.add_dll_directory(clidriver_path)
    
from infrastructure.adapters.in_.ui.streamlit_app import run_app


if __name__ == "__main__":
    """
    Punto de entrada para ejecutar la aplicación Streamlit.
    Para ejecutar:
    1. Asegúrate de estar en el directorio raíz del proyecto (multi-spaces).
    2. Ejecuta el comando: streamlit run app_launcher.py
    """
    run_app()
