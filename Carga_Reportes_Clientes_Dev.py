from urllib.parse import quote_plus
from pathlib import Path
from loguru import logger
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, TaskProgressColumn
from sqlalchemy import create_engine
from sqlalchemy.sql import text
# from contextlib import suppress
# from openpyxl import load_workbook
import polars as pl
import fastexcel
import datetime
# import chardet
from charset_normalizer import from_bytes
# import os, sys, shutil, tempfile
# import xlrd, time
import sys, socket, getpass, math, shutil, tempfile
from config import settings
# from decouple import config
# import warnings

# pyinstaller --noconfirm --onefile Carga_Reportes_Clientes_Dev.py --icon "Recursos/logo.ico"
# --hidden-import pyarrow.vendored.version
# uv run pyinstaller --noconfirm --onefile --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Clientes_V1.py 
# uv run pyinstaller --noconfirm --onedir --noupx --strip --icon "Recursos/logo.ico" --hidden-import fastexcel Carga_Reportes_Clientes_V1.py 
#--clean --log-level=DEBUG 

USUARIO = f"{socket.gethostname()}/{getpass.getuser()}"
HORA_INICIAL, HORA_FINAL = datetime.datetime.now(), datetime.datetime.now()
PERIODO = str(HORA_INICIAL.year) + str(HORA_INICIAL.month).zfill(2) + str(HORA_INICIAL.day).zfill(2)
if getattr(sys, 'frozen', False): 
    PATH_GENERAL = Path(sys.executable).resolve().parent 
else:
    PATH_GENERAL = Path(__file__).resolve().parent 
PATH_SOURCE_SD = PATH_GENERAL / 'Reporte_SD' 
PATH_SOURCE_OS = PATH_GENERAL / 'Reporte_OS' 
# PATH_LOG = PATH_GENERAL / 'LogApp_{time:YYYYMMDD}.log'
PATH_LOG = PATH_GENERAL / 'Logs' / f'LogApp_{PERIODO}.log'
FILE_LOG_EXISTS = False
TYPE_PROCESS_CSV = 0
# COLUMNS_DEFAULT_SD = []
# "TIPO_DOCUMENTO"	"PID"	"NOMBRE"	"AP_PATERNO"	"AP_MATERNO"	"RAZON_SOCIAL"	"F_NACIMIENTO"	
# "TIPO_ENTIDAD"	"F_REGISTRO"	"FUENTE_DATO"	"NACIONALIDAD"	"NOM_REL"	"AGEN_FAMIL"	"NOMBRE_BK"	
# "VALIDO_DESDE"	"VALIDO_HASTA"	"USU"	"REGISTRATION_DATE"
COLUMNS_INDEX_SD = [0,1,5,6,7,8,9,10,11,12,13,14,15,16,17]
COLUMNS_SD = ['TIPO_DOC','NRO_DOC','NOMBRE_CLIENTE','FECHA_NAC','TIPO_ENTIDAD','FECHA_REG','FUENTE','NACIONALIDAD','RAMOS',
              'COD_BROKER','BROKER','VALIDO_DESDE','VALIDO_HASTA','USUARIO','REGISTRATION_DATE']
COLUMNS_STRUCT_SD = ['NOMBRE_CLIENTE','FECHA_NAC','TIPO_ENTIDAD','FECHA_REG','FUENTE','NACIONALIDAD','RAMOS','COD_BROKER',
                  'BROKER','VALIDO_DESDE','VALIDO_HASTA','USUARIO','REGISTRATION_DATE']
# DROP_COLUMNS_SD = [2,3,4]
REMOVE_COLUMNS_SD = ['VALIDO_DESDE','VALIDO_HASTA','REGISTRATION_DATE']
COLUMNS_INDEX_OS = [0,1,2,3,4,5,6,7,8,9]
COLUMNS_OS = ['ID','TIPO_DOC','NRO_DOC','NOMBRE_CLIENTE','FECHA_CREA','FECHA_CARTA','BROKER',
              'USUARIO_CREA','USUARIO_CARTA','ESTADO_CARTA']
STOP_PROCESS = False
# FECHA_DATA DATETIME NULL,
# USUARIO_DATA VARCHAR(20) NULL
# COLUMNS_DATE = ['F_ANULACION_CERT','F_INI_COBER','F_FIN_COBERT']
# FORMATS_DATE_FMT8 = ["%d/%m/%y","%d-%m-%y", "%y/%m/%d", "%y-%m-%d"]
# FORMATS_DATE_FMT10 = ["%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d"]
# ROWS_LIMIT = 10_000_000


def show_custom_rule(titulo, state='Success'):
    ancho_total = console.width
    if state == "Success":
        color_linea = "bold green"
        color_texto = "grey66"
    elif state == "Error":
        color_linea = "red"
        color_texto = "grey66"
    else:
        color_linea = "cyan"
        color_texto = "grey66"

    texto = f" {titulo} "
    largo_texto = len(Text.from_markup(texto).plain)

    largo_linea = max((ancho_total - largo_texto) // 2, 0)
    linea = "─" * largo_linea

    regla = f"[{color_linea}]{linea}[/{color_linea}][{color_texto}]{texto}[/{color_texto}][{color_linea}]{linea}[/{color_linea}]"
    console.print(regla)
    
def custom_format(type_process: int):
    def formatter(record: dict):
        levelname = record['level'].name
        if levelname == 'INFO':
            text = 'AVISO'
            level_str = f'<cyan>{text:<7}</cyan>'
            message_color = '<cyan>'
        elif levelname == 'DEBUG':
            text = 'INFO'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        elif levelname == 'WARNING':
            text = 'ALERTA'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        elif levelname == 'SUCCESS':
            text = 'ÉXITO'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        else:
            level_str = f'<level>{levelname:<7}</level>'
            message_color = '<level>'
        
        original_message = str(record['message'])
        safe_message = original_message.replace("{", "{{").replace("}", "}}")
        custom_message = f"{message_color}{safe_message}</{message_color.strip('<>')}>\n"
        
        if type_process == 0:
            level_str = f'{level_str} | '
        else:
            level_str = f"{level_str} | {record['name']}:{record['function']}:{record['line']} - "
            if record["exception"] is not None:
                custom_message += f"{record['exception']}\n"

        return (
            f"<cyan><bold>{record['time']:DD/MM/YYYY HH:mm:ss}</bold></cyan> | "
            f"{level_str}"
            f"{custom_message}"
        )
    return formatter

def remove_log():
    logger.remove()

def add_log_console():
    logger.add(sys.stdout,
            backtrace=False, diagnose=False, level='DEBUG',
            colorize=True,
            format=custom_format(0))

def add_log_file(exits_log: bool):
    global FILE_LOG_EXISTS
    if PATH_LOG.exists() and not exits_log:
        logger.add(PATH_LOG, 
                backtrace=True, diagnose=True, level='DEBUG',
                format='\n\n{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
        return
    
    logger.add(PATH_LOG, 
        backtrace=True, diagnose=True, level='DEBUG',
        format='{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}') 
    FILE_LOG_EXISTS = True

def start_log(exits_log: bool = False):
    remove_log()
    add_log_console()
    add_log_file(exits_log)

class ConnectionDB_SQLServer_SQLAlchemy:
    def __init__(self, process_name: str):
        self.process_name = process_name
        self.driver=r'ODBC Driver 17 for SQL Server' #config('DEBUG', default=False, cast=bool)
        self.db_ip = settings.BD_IP 
        self.db_name = settings.BD_NAME
        self.Open_Connection()

    def create_connection(self):
        try:
            self.string_connection_pl = f'mssql+pyodbc://{self.db_ip}/{self.db_name}?driver={self.driver}&Trusted_Connection=yes'
            self.engine = create_engine(self.string_connection_pl, fast_executemany=True)
            self.conn = self.engine.connect()
            self.verification_date_register()
        except Exception as e:
            if 'timeout expired' in str(e).lower():
                raise Exception(f"{e}")
            try:
                self.db_user = settings.BD_USER_DEV_1
                self.db_password = settings.BD_PASSWORD_DEV_1
                encoded_password = quote_plus(self.db_password)

                self.string_connection_pl = f'mssql+pyodbc://{self.db_user}:{encoded_password}@{self.db_ip}/{self.db_name}?driver={self.driver}'
                self.engine = create_engine(self.string_connection_pl, fast_executemany=True)
                self.conn = self.engine.connect()
                self.verification_date_register()
            except Exception as e:
                self.db_user = settings.BD_USER_DEV_2
                self.db_password = settings.BD_PASSWORD_DEV_2
                encoded_password = quote_plus(self.db_password)

                self.string_connection_pl = f'mssql+pyodbc://{self.db_user}:{encoded_password}@{self.db_ip}/{self.db_name}?driver={self.driver}'
                self.engine = create_engine(self.string_connection_pl, fast_executemany=True)
                self.conn = self.engine.connect()
                self.verification_date_register()

    def Open_Connection(self):
        try:
            self.create_connection()
            self.result_date_max = self.result_date_max.date() if self.result_date_max is not None else datetime.date(1900,1,1)
        except Exception as e: 
            raise Exception(f"Error al abrir conexión a la base de datos.\n{e}") from e

    def Close_Connection(self):
        try:
            if self.conn:
                self.conn.close()
            if self.engine:
                self.engine.dispose()
        except Exception as e:
            raise Exception(f"Error al cerrar la conexión.\n{e}") from e

    def verification_date_register(self):
        stmt = text(f"SELECT MAX(FECHA_DATA) FROM [BD_PROYINTERNO].[CLI].[DATA_{self.process_name}]")
        self.cursor = self.conn.execute(stmt)
        self.result_date_max: datetime.datetime = self.cursor.fetchone()[0]

    def dataframe_to_table(self, df: pl.DataFrame, n_rows: int) -> str | int:
        global STOP_PROCESS
        estatus = 0
        try:  
            if self.result_date_max >= HORA_INICIAL.date():
                logger.warning(f"Ya se ha realizado una carga del Reporte {self.process_name} para el día de hoy.")
                validation = MenuPrompt.ask(
                    "[bold white]Desea continuar? (1: Sí, 2: No)[/bold white]", 
                    choices=["1", "2"]
                )
                try:
                    self.process = int(validation)
                    if self.process != 1:  
                        STOP_PROCESS = True
                        return      
                except Exception:
                    STOP_PROCESS = True
                    logger.error('Seleccione una opción válida.')
                    return
            
            # if STOP_PROCESS:
            #     logger.info(f"Total Registros (Reporte {process_name}): {n_rows}...")
            # else:
            #     logger.info(f"Total Registros Cargados (Reporte {process_name}): {n_rows}...") 
            
            # logger.info(f'Iniciando Exportación del Reporte {self.process_name} al Datamart...')  
            logger.info(f'Cargando Reporte {self.process_name} al Datamart. Total Registros: {n_rows}...') 

            min_chunk = 5_000
            chunk_size = max(math.ceil(0.2 * n_rows), min_chunk)

            if n_rows > (math.ceil(1.75 * min_chunk)):
                with Progress(
                    TextColumn("[bold blue]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    transient=True  # Limpia al finalizar
                ) as progress:
                    task = progress.add_task("Exportando al Datamart...", total=n_rows)
                     
                    for i in range(0, n_rows, chunk_size):
                        df_chunk = df[i:i + chunk_size] #df.slice(i, min(i + chunk_size, n_rows) - i)
                        df_chunk.write_database(table_name=f'CLI.DATA_{self.process_name}', connection=self.engine,
                                    if_table_exists='append')
                        # time.sleep(2.5)
                        progress.update(task, advance=df_chunk.height)
                        # progress.console.log(f"[green]✔️ Insertado chunk {i}-{i + chunk_size} ({df_chunk.height} registros)[/green]")
            else:
                df.write_database(table_name=f'CLI.DATA_{self.process_name}', connection=self.engine,
                                if_table_exists='append')    
            
            # logger.info(f'Fin Exportación del Reporte {self.process_name} al Datamart...') 
        except Exception as e: 
            estatus = f"Error al cargar el reporte al Datamart.\n{e}"
        finally:
            self.Close_Connection()
            return estatus
        
class MenuPrompt(IntPrompt):
    validate_error_message = "[red]⛔ Error:[/red] Por favor ingrese un número válido."
    
    illegal_choice_message = (
        "[red]⛔ Error:[/red] Por favor seleccione una de las opciones disponibles."
    )

class Process_ETL:
    def __init__(self, process_type: str):
        try:
            self.process = int(process_type)
            if self.process not in [1,2,3]:
                raise Exception()
            self.Process_Start()
        except Exception:
            console.print()
            logger.error('Escriba una valor válido.')
            print("[grey66]Presiona Enter para salir...[/grey66]")
            input()
            sys.exit(1)
                
    def Detect_encoding(self, file_path: Path, read_size: int = 200_000) -> str:
        try:
            with open(file_path, 'rb') as f:
                raw = f.read(read_size)
            result = from_bytes(raw).best()
            encoding = result.encoding if result else None

            if encoding is None or encoding.lower() in ('johab', 'utf-16', 'utf-32'):
                return 'ISO-8859-1'
            
            return encoding
        except Exception:
            return 'ISO-8859-1'

    def Read_CSV(self, process_name: str, csv_path: Path, columns_names: list[str]) -> pl.LazyFrame:
        def clear_csv(path: Path, encoding: str = 'utf-8') -> bool | None:
            try:
                with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, dir=path.parent) as temp_file:
                    with open(path, 'r', encoding=encoding, errors='strict') as original:
                        for line in original:
                            if line.startswith('"') and line.endswith('"\n'):
                                line = line[1:-2] + '\n'
                            elif line.startswith('"') and line.endswith('"'):
                                line = line[1:-1]
                            line = line.replace('""', '"')
                            temp_file.write(line)

                shutil.move(temp_file.name, path)
                
                return True
            except:
                return None

        def try_read_lazy(path: Path, encoding: str) -> pl.LazyFrame | None:
            global TYPE_PROCESS_CSV
            try:
                TYPE_PROCESS_CSV = 1
                encoding = 'utf8' if encoding.lower() == 'utf-8' else encoding
                lf = pl.scan_csv(path, infer_schema_length=0, truncate_ragged_lines=True)
                n_rows = lf.limit(1).collect(engine='streaming').height
                return lf if n_rows > 0 else None
            except:
                status = clear_csv(path, encoding)
                if status is None:
                    raise Exception(f"Error al limpiar el archivo CSV. Codificación: {encoding}")
                try:
                    print("process 2")
                    TYPE_PROCESS_CSV = 2
                    return pl.scan_csv(path, infer_schema_length=0, truncate_ragged_lines=True) 
                except:
                    print("process 3")
                    TYPE_PROCESS_CSV = 3
                    df = pl.read_csv(path, infer_schema_length=0, truncate_ragged_lines=True) #, encoding=encoding, low_memory=True
                    # print(df.estimated_size("mb"), "MB")
                    lf = df.lazy()
                    df.clear()
                    del df
                    # gc.collect()
                    return lf

        ct_encoding = self.Detect_encoding(csv_path)
        # print(ct_encoding)

        try:
            lf = try_read_lazy(csv_path, ct_encoding)
            columns_originales = lf.collect_schema().names()

            # if process_name == 'SD':
            #     columns_drop = [columns_originales[i] for i in DROP_COLUMNS_SD]
            #     lf = (
            #         lf
            #         .drop(columns_drop)
            #     )
            #     columns_originales = lf.collect_schema().names()

            if len(columns_originales) != len(columns_names):
                raise ValueError(f"Cantidad de columnas incorrecta. Permitido: {len(columns_names)}")

            mapping = dict(zip(columns_originales, columns_names))
            lf = (
                lf
                .with_columns([
                    pl.col(col).str.strip_chars() for col in columns_originales
                ])
                .rename(mapping)
            )

            if TYPE_PROCESS_CSV > 1:
                n_rows = lf.select(columns_names).limit(1).collect(engine='streaming').height
            else:
                n_rows = 1

            return lf if n_rows > 0 else None
        except Exception as e:
            raise Exception(f"{e}\nUbicación Archivo CSV: ./{csv_path}") from e

    def Read_Excel(self, process_name: str, excel_path: Path, columns_index_original: list[int], columns_names: list[str]) -> pl.LazyFrame | None:
        try:
            lf_final = []
            # try:
            #     workbook = load_workbook(excel_path, read_only=True) 
            #     sheet_names = workbook.sheetnames
            #     workbook.close()
            #     self.type_excel = 'xlsx'
            # except:
            #     with xlrd.open_workbook(excel_path) as load_xls:
            #         sheet_names = load_xls.sheet_names()
            #     self.type_excel = 'xls'
            
            self.type_excel = excel_path.suffix.lower().replace('.','')
            print(self.type_excel)

            reader = fastexcel.read_excel(excel_path)
            dtypes_map = {idx: "string" for idx in columns_index_original}

            for name in reader.sheet_names:
                # try:
                # warnings.filterwarnings('ignore')
                # q = (
                #     pl.read_excel(excel_path, sheet_name=name, 
                #                     infer_schema_length=0,
                #     ).lazy()
                # )
                # warnings.resetwarnings()
                sheet = reader.load_sheet_by_name(name,use_columns=columns_index_original,dtypes=dtypes_map)
                q = sheet.to_polars().lazy()

                columns_originales = q.collect_schema().names()

                # if process_name == 'SD':
                #     columns_drop = [columns_originales[i] for i in DROP_COLUMNS_SD]
                #     q = (
                #         q
                #         .drop(columns_drop)
                #     )
                #     columns_originales = q.collect_schema().names()

                if len(columns_originales) != len(columns_names):
                    raise ValueError(f"Cantidad de columnas incorrecta. Permitido: {len(columns_names)}")
                
                mapping = dict(zip(columns_originales, columns_names))

                q = (
                    q
                    .with_columns([
                        pl.col(col).str.strip_chars() for col in columns_originales
                    ])
                    .rename(mapping)
                )

                # df = q.collect(engine='streaming')
                # except Exception as e:
                #     logger.warning(f"No se pudo leer contenido de la hoja '{name}', se omite.\n{e}\nArchivo Excel : ./{excel_path.parent.name}/{excel_path.name}")
                #     continue
                
                # df_lazy = df.lazy()
                # lf_final.append(df_lazy)
                lf_final.append(q)
                q.clear()

            lf: pl.LazyFrame = pl.concat(lf_final)
            n_rows = lf.limit(1).collect(engine='streaming').height
            return lf if n_rows > 0 else None    
        except Exception as e:
            raise Exception(f"{e}\nUbicación Archivo Excel: {excel_path}") from e

    def Transform_Dataframe_SD_CSV(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        global COLUMNS_SD

        # self.total_rows = lf.select(pl.len()).collect(engine='streaming').item()
        logger.info(f"Transformando datos de Reporte SD...")

        def date_valid(col):
            return (
                    col.str.slice(0, 10).str.contains(r"^(?:\d{2}[-/]\d{2}[-/]\d{4})?$", literal=False)
                )

        q = (
            lf
            .with_columns(pl
                        .when(
                            date_valid(pl.col('FUENTE'))  &
                            ~ date_valid(pl.col('VALIDO_DESDE'))  &
                            date_valid(pl.col('VALIDO_HASTA'))
                        )
                        .then(pl.struct([
                            pl.concat_str([
                                pl.col('NOMBRE_CLIENTE'),
                                pl.lit(' '), 
                                pl.col('FECHA_NAC')
                            ]).alias('NOMBRE_CLIENTE'), 
                            pl.col('TIPO_ENTIDAD').alias('FECHA_NAC'),
                            pl.col('FECHA_REG').alias('TIPO_ENTIDAD'),
                            pl.col('FUENTE').alias('FECHA_REG'),
                            pl.col('NACIONALIDAD').alias('FUENTE'),
                            pl.col('RAMOS').alias('NACIONALIDAD'),
                            pl.col('COD_BROKER').alias('RAMOS'),
                            pl.col('BROKER').alias('COD_BROKER'),
                            pl.col('VALIDO_DESDE').alias('BROKER'),
                            pl.col('VALIDO_HASTA').alias('VALIDO_DESDE'),
                            pl.col('USUARIO').alias('VALIDO_HASTA'),
                            pl.col('REGISTRATION_DATE').alias('USUARIO'),
                            pl.lit(None).alias('REGISTRATION_DATE'),
                        ]))
                        .when(
                            date_valid(pl.col('FUENTE'))  &
                            ~ date_valid(pl.col('VALIDO_DESDE'))  &
                            ~ date_valid(pl.col('VALIDO_HASTA'))
                        )
                        .then(pl.struct([
                            pl.concat_str([
                                pl.col('NOMBRE_CLIENTE'),
                                pl.lit(' '), 
                                pl.col('FECHA_NAC')
                            ]).alias('NOMBRE_CLIENTE'), 
                            pl.col('TIPO_ENTIDAD').alias('FECHA_NAC'),
                            pl.col('FECHA_REG').alias('TIPO_ENTIDAD'),
                            pl.col('FUENTE').alias('FECHA_REG'),
                            pl.col('NACIONALIDAD').alias('FUENTE'),
                            pl.col('RAMOS').alias('NACIONALIDAD'),
                            pl.col('COD_BROKER').alias('RAMOS'),
                            pl.col('BROKER').alias('COD_BROKER'),
                            pl.concat_str([
                                pl.col('VALIDO_DESDE'),
                                pl.lit(' '), 
                                pl.col('VALIDO_HASTA')
                            ]).alias('BROKER'), 
                            pl.col('USUARIO').alias('VALIDO_DESDE'),
                            pl.col('REGISTRATION_DATE').alias('VALIDO_HASTA'),
                            pl.lit(None).alias('USUARIO'),
                            pl.lit(None).alias('REGISTRATION_DATE'),
                        ]))
                        .when(
                            date_valid(pl.col('FECHA_REG'))  &
                            ~ date_valid(pl.col('VALIDO_DESDE'))
                        )
                        .then(pl.struct([
                            pl.col('NOMBRE_CLIENTE').alias('NOMBRE_CLIENTE'),
                            pl.col('FECHA_NAC').alias('FECHA_NAC'),
                            pl.col('TIPO_ENTIDAD').alias('TIPO_ENTIDAD' ),
                            pl.col('FECHA_REG').alias('FECHA_REG'),
                            pl.col('FUENTE').alias('FUENTE'),
                            pl.col('NACIONALIDAD').alias('NACIONALIDAD'),
                            pl.col('RAMOS').alias('RAMOS'),
                            pl.col('COD_BROKER').alias('COD_BROKER'),
                            pl.concat_str([
                                pl.col('BROKER'),
                                pl.lit(' '), 
                                pl.col('VALIDO_DESDE')
                            ]).alias('BROKER'), 
                            pl.col('VALIDO_HASTA').alias('VALIDO_DESDE'),
                            pl.col('USUARIO').alias('VALIDO_HASTA'),
                            pl.col('REGISTRATION_DATE').alias('USUARIO'),
                            pl.lit(None).alias('REGISTRATION_DATE'),
                        ]))
                        .when(
                            date_valid(pl.col('NACIONALIDAD')) &
                            date_valid(pl.col('USUARIO'))
                        )
                        .then(pl.struct([
                            pl.concat_str([
                                pl.col('NOMBRE_CLIENTE'),
                                pl.lit(','), 
                                pl.col('FECHA_NAC'),
                                pl.lit(','), 
                                pl.col('TIPO_ENTIDAD'),
                            ]).alias('NOMBRE_CLIENTE'), 
                            pl.col('FECHA_REG').alias('FECHA_NAC'),
                            pl.col('FUENTE').alias('TIPO_ENTIDAD'),
                            pl.col('NACIONALIDAD').alias('FECHA_REG'),
                            pl.col('RAMOS').alias('FUENTE'),
                            pl.col('COD_BROKER').alias('NACIONALIDAD'),
                            pl.col('BROKER').alias('RAMOS'),
                            pl.col('VALIDO_DESDE').alias('COD_BROKER'),
                            pl.col('VALIDO_HASTA').alias('BROKER'),
                            pl.col('USUARIO').alias('VALIDO_DESDE'),
                            pl.col('REGISTRATION_DATE').alias('VALIDO_HASTA'),
                            pl.lit(None).alias('USUARIO'),
                            pl.lit(None).alias('REGISTRATION_DATE'),
                        ]))
                        .when(
                            date_valid(pl.col('NACIONALIDAD')) &
                            ~ date_valid(pl.col('USUARIO')) 
                        )
                        .then(pl.struct([
                            pl.concat_str([
                                pl.col('NOMBRE_CLIENTE'),
                                pl.lit(','), 
                                pl.col('FECHA_NAC'),
                                pl.lit(','), 
                                pl.col('TIPO_ENTIDAD'),
                            ]).alias('NOMBRE_CLIENTE'), 
                            pl.col('FECHA_REG').alias('FECHA_NAC'),
                            pl.col('FUENTE').alias('TIPO_ENTIDAD'),
                            pl.col('NACIONALIDAD').alias('FECHA_REG'),
                            pl.col('RAMOS').alias('FUENTE'),
                            pl.col('COD_BROKER').alias('NACIONALIDAD'),
                            pl.col('BROKER').alias('RAMOS'),
                            pl.col('VALIDO_DESDE').alias('COD_BROKER'),
                            pl.concat_str([
                                pl.col('VALIDO_HASTA'),
                                pl.lit(','), 
                                pl.col('USUARIO'),
                            ]).alias('BROKER'), 
                            pl.col('REGISTRATION_DATE').alias('VALIDO_DESDE'),
                            pl.lit(None).alias('VALIDO_HASTA'),
                            pl.lit(None).alias('USUARIO'),
                            pl.lit(None).alias('REGISTRATION_DATE'),
                        ]))
                        .otherwise(pl.struct(COLUMNS_STRUCT_SD)).alias('custom_struct'))
            .drop(COLUMNS_STRUCT_SD)
            .unnest('custom_struct')
        )
        
        q = (
            q
            .with_columns(
                pl.when((pl.col('TIPO_DOC').str.contains('RUC', literal=True, strict=False)))
                .then(pl.lit('RUC'))
                .when((pl.col('TIPO_DOC').str.contains('PAS', literal=True, strict=False)))
                .then(pl.lit('PT'))
                .when((pl.col('TIPO_DOC').str.contains('CARNET', literal=True, strict=False)))
                .then(pl.lit('CE'))
                .otherwise(pl.col('TIPO_DOC')).alias('TIPO_DOC')
            )
            .with_columns(
                pl.when(
                        (pl.col('VALIDO_HASTA').is_null()) | 
                        (pl.col('VALIDO_HASTA') == '')
                    )
                .then(pl.lit('A'))
                .otherwise(pl.lit('C')).alias('AGENCIAMIENTO')
            )
            .with_columns(pl.col('FECHA_NAC').str.slice(0, 10).str.strptime(pl.Date, format="%d/%m/%Y", strict=False).cast(pl.Date))
            .with_columns(pl.col('FECHA_REG').str.slice(0, 10).str.strptime(pl.Date, format="%d/%m/%Y").cast(pl.Datetime))
            .with_columns(pl.lit(datetime.datetime.today()).cast(pl.Datetime).alias('FECHA_DATA'))
            .with_columns(pl.lit(USUARIO).alias('USUARIO_DATA')) 
        )

        self.list_max_date.append(q.select(pl.col('FECHA_REG').max().cast(pl.Date)).collect(engine='streaming').item())

        self.excluded_rows = self.excluded_rows + q.filter((pl.col('FECHA_REG').cast(pl.Date) > self.date_final_process)).select(pl.len()).collect(engine='streaming').item()
        q = q.filter((pl.col('FECHA_REG').cast(pl.Date) <= self.date_final_process))

        # print(q.
        #       filter(
        #                 (pl.col('TIPO_DOC') == 'DNI') 
        #                 # | 
        #                 # (pl.col('FECHA_NAC') == '')
        #             )
        #       .select(['NOMBRE_CLIENTE','FECHA_NAC','TIPO_ENTIDAD','FECHA_REG','FUENTE',
        #                'VALIDO_DESDE','VALIDO_HASTA','USUARIO','REGISTRATION_DATE'])
        #       .sort('FECHA_REG', descending=True)
        #       .collect()
        #       .head(20)
        # )

        COLUMNS_SD = [a for a in COLUMNS_SD if a not in REMOVE_COLUMNS_SD]
        COLUMNS_SD.append('AGENCIAMIENTO')
        COLUMNS_SD.append('FECHA_DATA')
        COLUMNS_SD.append('USUARIO_DATA')

        q = (
            q
            .drop(REMOVE_COLUMNS_SD)
            .unique(subset=COLUMNS_SD)
            .select(COLUMNS_SD)
        )

        return q

    def Transform_Dataframe_SD_Excel(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        global COLUMNS_SD

        # self.total_rows = lf.select(pl.len()).collect(engine='streaming').item()
        logger.info(f"Transformando datos de Reporte SD...")

        q = (
            lf
            .with_columns(
                pl.when((pl.col('TIPO_DOC').str.contains('RUC', literal=True, strict=False)))
                .then(pl.lit('RUC'))
                .when((pl.col('TIPO_DOC').str.contains('PAS', literal=True, strict=False)))
                .then(pl.lit('PT'))
                .when((pl.col('TIPO_DOC').str.contains('CARNET', literal=True, strict=False)))
                .then(pl.lit('CE'))
                .otherwise(pl.col('TIPO_DOC')).alias('TIPO_DOC')
            )
            .with_columns(
                pl.when(
                        (pl.col('VALIDO_HASTA').is_null()) | 
                        (pl.col('VALIDO_HASTA') == '')
                    )
                .then(pl.lit('A'))
                .otherwise(pl.lit('C')).alias('AGENCIAMIENTO')
            )
        )
        
        if self.type_excel == 'xlsx':
            q = (
                q
                .with_columns(pl.col('FECHA_NAC').str.slice(0, 10).str.strptime(pl.Date, format="%Y-%m-%d", strict=False).cast(pl.Date))
                .with_columns(pl.col('FECHA_REG').str.slice(0, 19).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S").cast(pl.Datetime))
                .with_columns(pl.lit(datetime.datetime.today()).cast(pl.Datetime).alias('FECHA_DATA'))
                .with_columns(pl.lit(USUARIO).alias('USUARIO_DATA'))
            )
        else:
            q = (
                q
                .with_columns(pl.col('FECHA_NAC').str.split('.').list.get(0).cast(pl.Int32).alias('FECHA_NAC'))
                .with_columns(('0.' + pl.col('FECHA_REG').str.split('.').list.get(-1)).cast(pl.Float32).alias('FECHA_REG_DECIMAL'))
                .with_columns((pl.col('FECHA_REG_DECIMAL') * 86400).cast(pl.Int32).alias('FECHA_REG_SEG'))
                .with_columns(pl.col('FECHA_REG').str.split('.').list.get(0).cast(pl.Int32).alias('FECHA_REG'))
                .with_columns((pl.duration(days=pl.col('FECHA_NAC')) + datetime.date(1900,1,1) - pl.duration(days=2)).cast(pl.Date).alias('FECHA_NAC'))
                .with_columns((pl.duration(days=pl.col('FECHA_REG'),seconds=pl.col('FECHA_REG_SEG')) + datetime.datetime(1900,1,1) - pl.duration(days=2)).cast(pl.Datetime).alias('FECHA_REG'))
                .with_columns(pl.lit(datetime.datetime.today()).cast(pl.Datetime).alias('FECHA_DATA'))
                .with_columns(pl.lit(USUARIO).alias('USUARIO_DATA'))
                .drop(['FECHA_REG_DECIMAL','FECHA_REG_SEG'])
            )

        # print(q
        #     #   .filter( pl.col('NRO_DOC') == '20306797256')
        #       .select(['TIPO_DOC','NRO_DOC','NOMBRE_CLIENTE','FECHA_NAC','TIPO_ENTIDAD','FECHA_REG',
        #                 'COD_BROKER','BROKER','VALIDO_DESDE','VALIDO_DESDE_BK']).sort('FECHA_REG', descending=True).collect().head(20))
        
        self.list_max_date.append(q.select(pl.col('FECHA_REG').max().cast(pl.Date)).collect(engine='streaming').item())

        self.excluded_rows = self.excluded_rows + q.filter((pl.col('FECHA_REG').cast(pl.Date) > self.date_final_process)).select(pl.len()).collect(engine='streaming').item()
        q = q.filter((pl.col('FECHA_REG').cast(pl.Date) <= self.date_final_process))

        COLUMNS_SD = [a for a in COLUMNS_SD if a not in REMOVE_COLUMNS_SD]
        COLUMNS_SD.append('AGENCIAMIENTO')
        COLUMNS_SD.append('FECHA_DATA')
        COLUMNS_SD.append('USUARIO_DATA')

        q = (
            q
            .drop(REMOVE_COLUMNS_SD)
            .unique(subset=COLUMNS_SD)
            .select(COLUMNS_SD)
        )

        return q    

    def Transform_Dataframe_OS(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        
        # self.total_rows = lf.select(pl.len()).collect(engine='streaming').item()
        logger.info(f"Transformando datos de Reporte OS...")

        q = (
            lf
            .with_columns(pl.col('ID').cast(pl.Int32).alias('ID')) 
            .with_columns(pl.col('FECHA_CREA').str.slice(0, 10).str.strptime(pl.Date, format="%d/%m/%Y").cast(pl.Date))
            .with_columns(pl.col('FECHA_CARTA').str.slice(0, 10).str.strptime(pl.Date, format="%d/%m/%Y", strict=False).cast(pl.Date))
            .with_columns(pl.lit(datetime.datetime.today()).cast(pl.Datetime).alias('FECHA_DATA')) 
            .with_columns(pl.lit(USUARIO).alias('USUARIO_DATA')) 
            .with_columns(
                pl.when((pl.col('USUARIO_CREA') == ''))
                .then(pl.lit(None))
                .otherwise(pl.col('USUARIO_CREA')).alias('USUARIO_CREA')
            )
            .with_columns(
                pl.when((pl.col('USUARIO_CARTA') == ''))
                .then(pl.lit(None))
                .otherwise(pl.col('USUARIO_CARTA')).alias('USUARIO_CARTA')
            )
        )

        self.list_max_date.append(q.select(pl.col('FECHA_CREA').max().cast(pl.Date)).collect(engine='streaming').item())

        self.excluded_rows = self.excluded_rows + q.filter((pl.col('FECHA_CREA') > self.date_final_process)).select(pl.len()).collect(engine='streaming').item()
        q = q.filter((pl.col('FECHA_CREA') <= self.date_final_process))

        COLUMNS_OS.append('FECHA_DATA')
        COLUMNS_OS.append('USUARIO_DATA')

        q = (
            q
            .unique(subset=COLUMNS_OS)
            .select(COLUMNS_OS)
        )

        return q
    
    def Export_Dataframe_SQL_Server(self, process_name: str, list_df: list):
        try:
            logger.info(f'Consolidando información de Reporte {process_name}...')
            q: pl.LazyFrame = pl.concat(list_df)
            n_rows = q.select(pl.len()).collect(engine='streaming').item()
            list_df.clear()

            column_date = 'FECHA_REG' if process_name == 'SD' else 'FECHA_CREA'
            min_date = q.select(pl.col(column_date).min().cast(pl.Date)).collect(engine='streaming').item()
            if self.excluded_rows > 0:
                logger.warning(f'Primera Fecha de Registro {process_name}: {datetime.datetime.strftime(min_date,"%d-%m-%Y")}...')
                logger.warning(f'Última Fecha de Registro {process_name}: {datetime.datetime.strftime(max(self.list_max_date),"%d-%m-%Y")}...')
                logger.warning(f'Total Registros Excluidos {process_name}: {self.excluded_rows}...')
            else:
                logger.debug(f'Primera Fecha de Registro {process_name}: {datetime.datetime.strftime(min_date,"%d-%m-%Y")}...')
                logger.debug(f'Última Fecha de Registro {process_name}: {datetime.datetime.strftime(max(self.list_max_date),"%d-%m-%Y")}...')

            logger.info(q.collect_schema())
            print(f"Total de Registros : {n_rows}")
            if process_name == 'SD':
                print(q.select(['TIPO_DOC','NRO_DOC','NOMBRE_CLIENTE','FECHA_NAC','TIPO_ENTIDAD','FECHA_REG',
                                'COD_BROKER','BROKER']).sort('FECHA_REG', descending=True).collect().head(20))
            else:
                print(q.select(['ID','TIPO_DOC','NRO_DOC','NOMBRE_CLIENTE','FECHA_CREA','BROKER'])
                      .sort('FECHA_CREA', descending=True).collect().head(20))

            logger.info(f'Conectando al Datamart...')
            conexiondb_sqlserver = ConnectionDB_SQLServer_SQLAlchemy(process_name)

            status = conexiondb_sqlserver.dataframe_to_table(q.collect(),n_rows)

            if isinstance(status, str):
                if "Error" in status:
                    raise Exception(status)
            
            q.clear() 
        except Exception as e:
            raise Exception(f"Error al consolidar información.\n{e}") from e 

    def Process_Start(self):
        global HORA_INICIAL, HORA_FINAL
        
        self.date_final_process = Prompt.ask("[bold white]Escriba la Fecha de Corte (DD-MM-YYYY)[/bold white]")
        self.date_final_process = datetime.datetime.strptime(self.date_final_process,"%d-%m-%Y").date()
        # self.date_final_process = datetime.datetime.strptime("01-08-2025","%d-%m-%Y").date()
        self.list_max_date: list = []

        HORA_INICIAL = datetime.datetime.now()
        nombres = {"1": "Cargar Reporte SD", "2": "Cargar Reporte OS", "3": "Cargar Ambos Reportes (SD/OS)"}
        nombre_proceso = nombres.get(str(self.process).strip(), "Proceso Desconocido")
        console.rule(f"[grey66]Proceso Iniciado: [bold white]{nombre_proceso}[/bold white][/grey66]")
        remove_log()
        if FILE_LOG_EXISTS:
            PATH_LOG.unlink(missing_ok=True)
        add_log_file(False)
        logger.info(f'Comienzo del Proceso {nombre_proceso}...')
        remove_log()
        start_log(True)
        try:
            if self.process == 1 or self.process == 3:
                if not PATH_SOURCE_SD.exists():
                    raise FileNotFoundError(f"La carpeta principal no existe o tiene un nombre diferente de 'Reporte_SD'.\nUbicación Carpeta Esperada: {PATH_SOURCE_SD}")
            
            if self.process == 2 or self.process == 3:
                if not PATH_SOURCE_OS.exists():
                    raise FileNotFoundError(f"La carpeta principal no existe o tiene un nombre diferente de 'Reporte_OS'.\nUbicación Carpeta Esperada: {PATH_SOURCE_OS}")          

            # def processing_csvs(process_name: str, csv_files_list: list[Path], columns_names: list[str]):
            #     for csv in csv_files_list:
            #         lf = self.Read_CSV(process_name, csv, columns_names)
            #         if lf is None:
            #             raise Exception(f"El archivo CSV no cuenta con información.\nUbicación Archivo CSV: {csv}")
            #         yield lf

            def processing_excels(process_name: str, excels_files_list: list[Path], columns_index_original: list[int], columns_names: list[str]):
                for excel in excels_files_list:
                    lf = self.Read_Excel(process_name, excel,  columns_index_original, columns_names)         
                    if lf is None:
                        raise Exception(f"El archivo Excel no cuenta con información.\nUbicación Archivo Excel: {excel}")
                    yield lf

            if self.process == 1 or self.process == 3:
                self.excluded_rows = 0
                self.list_max_date.clear()
                # lazyframes_folder_sntros = []
                # lazyframes = []
                logger.info('Recorriendo contenido de carpeta SD...')

                # excels = list(PATH_SOURCE_SNTROS.glob("*.xlsx")) + list(PATH_SOURCE_SNTROS.glob("*.xls"))
                # excels = [f for f in PATH_SOURCE_SD.iterdir() if f.suffix in ['.csv']]
                excels = [f for f in PATH_SOURCE_SD.iterdir() if f.suffix in ['.xlsx','.xls']]
                if not excels:
                    raise Exception(f"No se encontraron archivos Excels en carpeta principal.\nUbicación Carpeta SD: {PATH_SOURCE_SD}")

                # lf_final = self.Transform_Dataframe_SD_CSV(pl.concat(processing_csvs('SD',excels,COLUMNS_SD)))
                lf_final = self.Transform_Dataframe_SD_Excel(pl.concat(processing_excels('SD',excels,COLUMNS_INDEX_SD,COLUMNS_SD)))

                self.Export_Dataframe_SQL_Server('SD', [lf_final])

            if self.process == 2 or self.process == 3:
                self.excluded_rows = 0
                self.list_max_date.clear()
                # lazyframes_folder_sntros = []
                # lazyframes = []
                logger.info('Recorriendo contenido de carpeta OutSystems...')

                # excels = list(PATH_SOURCE_SNTROS.glob("*.xlsx")) + list(PATH_SOURCE_SNTROS.glob("*.xls"))
                excels = [f for f in PATH_SOURCE_OS.iterdir() if f.suffix in ['.xlsx','.xls']]
                if not excels:
                    raise Exception(f"No se encontraron archivos Excel en carpeta principal.\nUbicación Carpeta OutSystems: {PATH_SOURCE_OS}")

                lf_final = self.Transform_Dataframe_OS(pl.concat(processing_excels('OS',excels,COLUMNS_INDEX_OS,COLUMNS_OS)))

                # lazyframes_folder_sntros.append(q)
                # del q
                 
                self.Export_Dataframe_SQL_Server('OS', [lf_final])
                # lazyframes_folder_sntros = None

                # print(q.select(['ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                # 'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT']).sort(by='ASEGURADO').limit(20).collect().head(20))
                # q = q.select(['COD_RAMO','POLIZA','CERTIFICADO','ASEGURADO','TIPO_DOC','NRO_DOCUMENTO',
                #  'F_INI_COBER','F_FIN_COBERT','F_ANULACION_CERT']).sort(by='NRO_DOCUMENTO').collect()
                # print(q.filter(q.is_duplicated()).head(100))
                 # q = None

            self.list_max_date.clear()
            HORA_FINAL = datetime.datetime.now()
            if STOP_PROCESS:
                logger.warning('Ejecución Detenida: No se cargó la información.')
            else:
                logger.success('Ejecución exitosa: Se cargó la información.')
            difference_time = HORA_FINAL-HORA_INICIAL
            total_seconds = int(difference_time.total_seconds())
            difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)

            remove_log()
            add_log_file(True)
            logger.info(f'Tiempo de proceso: {difference_formated}')
            add_log_console()
            print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')

            console.rule(f"[grey66]Proceso Finalizado[/grey66]")
            print("[grey66]Presiona Enter para salir...[/grey66]")
            input()
            sys.exit(0)
        except Exception as e:
            HORA_FINAL = datetime.datetime.now()
            logger.error('Proceso Incompleto. Detalle: '+str(e))
            difference_time = HORA_FINAL-HORA_INICIAL
            total_seconds = int(difference_time.total_seconds())
            difference_formated = "{} minuto(s), {} segundo(s)".format((total_seconds // 60), total_seconds % 60)

            remove_log()
            add_log_file(True)
            logger.info(f'Tiempo de proceso: {difference_formated}')
            add_log_console()
            print(f'[dark_orange]Tiempo de proceso: {difference_formated}[/dark_orange]')

            show_custom_rule('Proceso Finalizado con Error', state='Error')
            print("[grey66]Presiona Enter para salir...[/grey66]")
            input()
            sys.exit(1)

if __name__=='__main__':
    start_log()
    console = Console()
    menu_text = (
        "[bold grey93]\nSeleccione el tipo de Proceso[/bold grey93]\n\n"
        "[cyan]1.[/] Cargar Reporte SD\n"
        "[cyan]2.[/] Cargar Reporte OS\n"
        "[cyan]3.[/] Cargar Ambos Reportes (SD/OS)\n"
    )
    console.print(Panel.fit(menu_text, title="[bold]Menú de Procesos[/bold]", border_style="grey50"))

    process_type = MenuPrompt.ask(
        "[bold white]Escriba el Nro de opción[/bold white]", 
        choices=["1", "2", "3"]
    )

    Process_ETL(process_type)

