import boto3
from boto3.dynamodb.conditions import Key, Attr
from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()
# Retrieve the environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
region_name = os.getenv('AWS_REGION')

# Initialize a session using Amazon DynamoDB
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=region_name
)

dynamodb = session.resource('dynamodb')
# 1 Create tables
def crear_tablas():
    tablas_existentes = dynamodb.tables.all()
    nombres_tablas_existentes = [tabla.name for tabla in tablas_existentes]
    if 'Pacientes' not in nombres_tablas_existentes:
        tabla_pacientes = dynamodb.create_table(
            TableName='Pacientes',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
                {'AttributeName': 'nombre', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
                {'AttributeName': 'nombre', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        tabla_pacientes.wait_until_exists()
        print("Tabla Pacientes creada.")
    else:
        print("Tabla Pacientes ya existe.")

    if 'Doctores' not in nombres_tablas_existentes:
        tabla_doctores = dynamodb.create_table(
            TableName='Doctores',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
                {'AttributeName': 'nombre', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
                {'AttributeName': 'nombre', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        tabla_doctores.wait_until_exists()
        print("Tabla Doctores creada.")
    else:
        print("Tabla Doctores ya existe.")

    if 'Citas' not in nombres_tablas_existentes:
        tabla_citas = dynamodb.create_table(
            TableName='Citas',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
                {'AttributeName': 'fecha', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
                {'AttributeName': 'fecha', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        tabla_citas.wait_until_exists()
        print("Tabla Citas creada.")
    else:
        print("Tabla Citas ya existe.")
#2 Insert records
def insert_records():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    # Insert records into Pacientes table
    for i in range(1, 4):
        item_id = str(i)
        item_nombre = f'Paciente_{item_id}'
        existing_item = pacientes_table.get_item(Key={'id': item_id, 'nombre': item_nombre})
        if 'Item' not in existing_item:
            pacientes_table.put_item(Item={'id': item_id, 'nombre': item_nombre, 'edad': 30 + i, 'genero': 'M'})
            print(f"Item {item_id} insertado en Pacientes.")
        else:
            print(f"Item {item_id} ya existe en Pacientes.")

    # Insert records into Doctores table
    for i in range(1, 4):
        item_id = str(i)
        item_nombre = f'Doctor_{item_id}'
        existing_item = doctores_table.get_item(Key={'id': item_id, 'nombre': item_nombre})
        if 'Item' not in existing_item:
            doctores_table.put_item(Item={'id': item_id, 'nombre': item_nombre, 'especialidad': 'Cardiología'})
            print(f"Item {item_id} insertado en Doctores.")
        else:
            print(f"Item {item_id} ya existe en Doctores.")

    # Insert records into Citas table
    for i in range(1, 4):
        item_id = str(i)
        item_fecha = f'2023-10-{10 + i}'
        existing_item = citas_table.get_item(Key={'id': item_id, 'fecha': item_fecha})
        if 'Item' not in existing_item:
            citas_table.put_item(Item={'id': item_id, 'paciente_id': str(i), 'doctor_id': str(i), 'fecha': item_fecha})
            print(f"Item {item_id} insertado en Citas.")
        else:
            print(f"Item {item_id} ya existe en Citas.")

# 3 Get a record
def get_record():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    response1 = pacientes_table.get_item(Key={'id': '1', 'nombre': 'Paciente_1'})
    response2 = doctores_table.get_item(Key={'id': '1', 'nombre': 'Doctor_1'})
    response3 = citas_table.get_item(Key={'id': '1', 'fecha': '2023-10-11'})

    print(response1.get('Item', 'No se encontró el item en Pacientes'))
    print(response2.get('Item', 'No se encontró el item en Doctores'))
    print(response3.get('Item', 'No se encontró el item en Citas'))

# # 4 Update a record
def update_record():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    existing_item_pacientes = pacientes_table.get_item(Key={'id': '1', 'nombre': 'Paciente_1'})
    if existing_item_pacientes['Item']['edad'] != 35:
        pacientes_table.update_item(
            Key={'id': '1', 'nombre': 'Paciente_1'},
            UpdateExpression='SET edad = :val1',
            ExpressionAttributeValues={':val1': 35}
        )
        print("Item actualizado en Pacientes.")
    else:
        print("Item ya está actualizado en Pacientes.")

    existing_item_doctores = doctores_table.get_item(Key={'id': '1', 'nombre': 'Doctor_1'})
    if existing_item_doctores['Item']['especialidad'] != 'Neurología':
        doctores_table.update_item(
            Key={'id': '1', 'nombre': 'Doctor_1'},
            UpdateExpression='SET especialidad = :val1',
            ExpressionAttributeValues={':val1': 'Neurología'}
        )
        print("Item actualizado en Doctores.")
    else:
        print("Item ya está actualizado en Doctores.")

    existing_item_citas = citas_table.get_item(Key={'id': '1', 'fecha': '2023-10-11'})
    if existing_item_citas['Item']['paciente_id'] != '2':
        citas_table.update_item(
            Key={'id': '1', 'fecha': '2023-10-11'},
            UpdateExpression='SET paciente_id = :val1',
            ExpressionAttributeValues={':val1': '2'}
        )
        print("Item actualizado en Citas.")
    else:
        print("Item ya está actualizado en Citas.")

# 5 Delete a record
def delete_record():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    existing_items_pacientes = pacientes_table.get_item(Key={'id': '1', 'nombre': 'Paciente_1'})
    if 'Item' in existing_items_pacientes:
        print("Item a borrar de Pacientes:", existing_items_pacientes['Item'])
        pacientes_table.delete_item(Key={'id': '1', 'nombre': 'Paciente_1'})
        print("Item borrado de Pacientes.")
    else:
        print("Item no existe en Pacientes.")

    existing_items_doctores = doctores_table.get_item(Key={'id': '1', 'nombre': 'Doctor_1'})
    if 'Item' in existing_items_doctores:
        print("Item a borrar de Doctores:", existing_items_doctores['Item'])
        doctores_table.delete_item(Key={'id': '1', 'nombre': 'Doctor_1'})
        print("Item borrado de Doctores.")
    else:
        print("Item no existe en Doctores.")

    existing_items_citas = citas_table.get_item(Key={'id': '1', 'fecha': '2023-10-11'})
    if 'Item' in existing_items_citas:
        print("Item a borrar de Citas:", existing_items_citas['Item'])
        citas_table.delete_item(Key={'id': '1', 'fecha': '2023-10-11'})
        print("Item borrado de Citas.")
    else:
        print("Item no existe en Citas.")
# 6 Obtener todos los registros
def get_all_records():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    response1 = pacientes_table.scan()
    response2 = doctores_table.scan()
    response3 = citas_table.scan()

    print(response1['Items'])
    print(response2['Items'])
    print(response3['Items'])

# 7 Filtered records
def get_filtered_records():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    response1 = pacientes_table.scan(FilterExpression=Attr('edad').gt(30))
    response2 = doctores_table.scan(FilterExpression=Attr('especialidad').eq('Cardiología'))
    response3 = citas_table.scan(FilterExpression=Attr('fecha').begins_with('2023-10'))

    print(response1['Items'])
    print(response2['Items'])
    print(response3['Items'])

# 8 Conditional delete
def conditional_delete():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    existing_item_pacientes = pacientes_table.get_item(Key={'id': '3', 'nombre': 'Paciente_3'})
    if 'Item' in existing_item_pacientes:
        pacientes_table.delete_item(
            Key={'id': '3', 'nombre': 'Paciente_3'},
            ConditionExpression=Attr('edad').eq(33)
        )
        print("Item borrado de Pacientes:", existing_item_pacientes['Item'])
    else:
        print("Item no existe en Pacientes.")

    existing_item_doctores = doctores_table.get_item(Key={'id': '3', 'nombre': 'Doctor_3'})
    if 'Item' in existing_item_doctores:
        doctores_table.delete_item(
            Key={'id': '3', 'nombre': 'Doctor_3'},
            ConditionExpression=Attr('especialidad').eq('Cardiología')
        )
        print("Item borrado de Doctores:", existing_item_doctores['Item'])
    else:
        print("Item no existe en Doctores.")

    existing_item_citas = citas_table.get_item(Key={'id': '3', 'fecha': '2023-10-13'})
    if 'Item' in existing_item_citas:
        citas_table.delete_item(
            Key={'id': '3', 'fecha': '2023-10-13'},
            ConditionExpression=Attr('paciente_id').eq('3')
        )
        print("Item borrado de Citas:", existing_item_citas['Item'])
    else:
        print("Item no existe en Citas.")

# 9 Multiple filters
def get_multiple_filters():
    pacientes_table = dynamodb.Table('Pacientes')
    doctores_table = dynamodb.Table('Doctores')
    citas_table = dynamodb.Table('Citas')

    filter_expression1 = Attr('genero').eq('M') & Attr('nombre').contains('e')
    filter_expression2 = Attr('nombre').begins_with('Doctor') & Attr('especialidad').eq('Cardiología')
    filter_expression3 = Attr('fecha').begins_with('2023-10') & Attr('doctor_id').eq('2')

    response1 = pacientes_table.scan(FilterExpression=filter_expression1)
    response2 = doctores_table.scan(FilterExpression=filter_expression2)
    response3 = citas_table.scan(FilterExpression=filter_expression3)

    print(f"Filtro Pacientes: Genero M y nombre contenga 'e'")
    print(response1['Items'])
    print(f"Filtro Doctores: Nombres empiezen con Doctor y especialidad Cardiología")
    print(response2['Items'])
    print(f"Filtro Citas: fecha empieze con 2023-10 y doctor_id sea 2")
    print(response3['Items'])

# # PartiQL statement
def partiql_statement():
    client = session.client('dynamodb')

    response1 = client.execute_statement(Statement="SELECT * FROM Pacientes WHERE id='2'")
    response2 = client.execute_statement(Statement="SELECT * FROM Doctores WHERE id='2'")
    response3 = client.execute_statement(Statement="SELECT * FROM Citas WHERE id='2'")

    print(response1['Items'])
    print(response2['Items'])
    print(response3['Items'])

# Crear copias de seguridad de las tablas
def backup_tables():
    client = session.client('dynamodb')

    # Verificar si ya existen copias de seguridad y eliminarlas
    existing_backups = client.list_backups(TableName='Pacientes')['BackupSummaries']
    for backup in existing_backups:
        client.delete_backup(BackupArn=backup['BackupArn'])

    existing_backups = client.list_backups(TableName='Doctores')['BackupSummaries']
    for backup in existing_backups:
        client.delete_backup(BackupArn=backup['BackupArn'])

    existing_backups = client.list_backups(TableName='Citas')['BackupSummaries']
    for backup in existing_backups:
        client.delete_backup(BackupArn=backup['BackupArn'])

    # Crear nuevas copias de seguridad
    client.create_backup(TableName='Pacientes', BackupName='PacientesBackup')
    client.create_backup(TableName='Doctores', BackupName='DoctoresBackup')
    client.create_backup(TableName='Citas', BackupName='CitasBackup')

if __name__ == "__main__":
    #print("Ejercicio 1: Crear tablas")
    #crear_tablas()
    
    #print("Ejercicio 2: Insertar registros")
    #insert_records()
    
    # print("Ejercicio 3: Obtener un registro")
    # get_record()
    
    # print("Ejercicio 4: Actualizar un registro")
    # update_record()
    
    # print("Ejercicio 5: Borrar un registro")
    # delete_record()
    
    # print("Ejercicio 6: Obtener todos los registros")
    # get_all_records()
    
    # print("Ejercicio 7: Obtener registros filtrados")
    # get_filtered_records()
    
    # print("Ejercicio 8: Borrado condicional")
    # conditional_delete()
    
    # print("Ejercicio 9: Obtener registros con múltiples filtros")
    # get_multiple_filters()
    
    # print("Ejercicio 10: Ejecutar declaración PartiQL")
    # partiql_statement()
    
    print("Ejercicio 11: Crear copias de seguridad de las tablas")
    backup_tables()