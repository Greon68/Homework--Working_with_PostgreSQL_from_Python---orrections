# Домашнее задание к лекции «Работа с PostgreSQL из Python» с исправлениями

# Создайте программу для управления клиентами на Python.
#
# Требуется хранить персональную информацию о клиентах:
#
# имя,
# фамилия,
# email,
# телефон.
# Сложность в том, что телефон у клиента может быть не один, а два, три и даже больше.
# А может и вообще не быть телефона, например, он не захотел его оставлять.
#
# Вам необходимо разработать структуру БД для хранения информации и несколько функций
# на Python для управления данными.
#
# 1)Функция, создающая структуру БД (таблицы).
# 2)Функция, позволяющая добавить нового клиента.
# 3)Функция, позволяющая добавить телефон для существующего клиента.
# 4)Функция, позволяющая изменить данные о клиенте.
# 5)Функция, позволяющая удалить телефон для существующего клиента.
# 6)Функция, позволяющая удалить существующего клиента.
# 7)Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.


import psycopg2

# 1)Функция, создающая структуру БД (таблицы).

def creating_tables (cursor):

    # создание таблицы client
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(60) NOT NULL ,
            last_name VARCHAR(60) NOT NULL ,
            email VARCHAR(120) UNIQUE NOT NULL 
            );
        """)
    # создание таблицы телефонов

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS phones (
                    phone_id SERIAL PRIMARY KEY,
                    client_id  INTEGER NOT NULL REFERENCES clients(client_id),
                    number INTEGER 
                    );
                """)
    conn.commit()  # фиксируем в БД


# 2) Функция, позволяющая добавить нового клиента.

def add_client(cursor, first_name, last_name, email):
        cursor.execute("""
        INSERT INTO clients (first_name, last_name, email )
        VALUES (%s, %s, %s);
        """ , (first_name, last_name, email ))


# 3) Функция, позволяющая добавить телефон для существующего клиента
def add_phone(cursor, client_id, number=None):
    cursor.execute("""
                INSERT INTO phones (client_id, number)
                VALUES (%s, %s );
                """, (client_id, number))


# 4)Функция, позволяющая изменить данные о клиенте.
#  Берём id клиента и меняем его : имя , фамилию и адрес эл. почты

def new_data(cursor, client_id, first_name=None, last_name=None, email=None):
    # Создадим возможность внесения изменений независимо друг от друга

    # Сформируем словарь 'название входного элемента':'значение входного элемента'
    data_in_dict = {'first_name': first_name, 'last_name': last_name, 'email': email, 'client_id': client_id}

    data = []  # рабочий список входных параметров

    data_out_0 = []  # Список выходных параметров

    for x, y in data_in_dict.items():
        if y != None:
            z = f'{x}=%s'
            data.append(z)
            data_out_0.append(y)
    # Удалим id из списка входных данных
    # print (data)
    data = data[:-1]
    data_in = ", ".join(data)  # строка входных данных
    data_out = tuple(data_out_0)  # кортеж выходных данных

    # print(data_in)
    # print(data_out)

    cursor.execute(f"""
                    UPDATE clients SET {data_in} WHERE client_id=%s;
                    """, data_out)


# 5)Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(cursor, client_id):
    cursor.execute("""
    DELETE FROM  phones WHERE client_id=%s;
    """, (client_id,))

# 6) Функция, позволяющая удалить существующего клиента.
def delete_client(cursor, client_id):
    # Удаляем клиента из таблицы phone
    delete_phone(cursor, client_id)
    # Удаляем клиента из таблицы clients
    cursor.execute("""
    DELETE FROM  clients WHERE client_id=%s;
    """, (client_id,))


 # 7) Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону


def find_client(cursor, first_name=None, last_name=None, email=None, number=None):
    ''' Функция определяет id клиента
    по  параметрам клиента - имени , фамилии , адресу электронной почты ,
    номеру его телефона . При этом возможны различные комбинации входных данных '''

    # Сформируем словарь 'название входного элемента':'значение входного элемента'
    data_in_dict = {'first_name': first_name, 'last_name': last_name, 'email': email, 'number': number}

    # Создадим возможность внесения изменений независимо друг от друга

    data = []  # рабочий список входных параметров
    data_out_0 = []  # Список выходных параметров

    # Проходим  циклом по словарю data_in_dict и отфильтровываем данные = None
    for key, val in data_in_dict.items():
        if val != None:
            z = f'{key}=%s'
            data.append(z)
            data_out_0.append(val)

    data_in = " AND ".join(data)  # строка входных данных
    data_out = tuple(data_out_0)  # кортеж выходных данных

    # print(data_in)
    # print(data_out)

   # Формируем  запрос , который работает с данными не равными None :
    cursor.execute(f"""
        SELECT cl.client_id FROM clients cl
        JOIN phones ph ON cl.client_id = ph.client_id
        WHERE {data_in} ;
        """, data_out)

    return cursor.fetchone()[0]

# Пароль для подключения - личный

with psycopg2.connect(database="client2", user="postgres", password="postgres") as conn:
    with conn.cursor() as cur:
        #удаление таблиц
        cur.execute("""
               DROP TABLE phones;
               DROP TABLE clients;
               """)

        # 1) Вызываем функцию создания таблиц
        tables= creating_tables(cur)



        # 2) Заполняем таблицу client

        client_1 = add_client ( cur ,'Anton','Petrov', 'anpet@python.com')
        client_2 = add_client ( cur ,'Petr','Ivanov', 'petr@python.com')
        client_3 =  add_client ( cur ,'Anton','Vasin', 'anton@python.com')
        client_4 =  add_client ( cur ,'Igor','Valin', 'ig@python.com')
        client_5 = add_client(cur, 'Irina', 'Li', 'li@python.com')
        client_6 = add_client(cur, 'Vlad', 'Ivanov', 'vlad@python.com')

        # 3) Вызов функции, позволяющей добавить телефон для существующего клиента

        # Заполняем таблицу  phones

        phone_1 = add_phone(cur, 1, 33322255)
        phone_2 = add_phone(cur,1,22233322)
        phone_3 = add_phone(cur,2)
        phone_4 = add_phone(cur, 3, 11122233)
        phone_5 = add_phone(cur, 3, 11122234)
        phone_6 = add_phone(cur, 4, 55522233)
        phone_7 = add_phone(cur, 5, 11100077)
        phone_8 = add_phone(cur, 6)



        # 4) Вызов функции, позволяющей изменить данные о клиенте.

        # new_client_1 = new_data(cur, 5, None, 'Zuza' , 'bos@python.com')
        # cur.execute(""" Select * From clients;""")
        # print(cur.fetchall())

        # 5) Вызов функции, позволяющей удалить телефон для существующего клиента.
        #   Удалим 2 номера телефона для клиента с id = 3 :

        # client_delete_phone_1 = delete_phone ( cur, 3 )


        # 6) Вызов функции, позволяющая удалить существующего клиента.

        # client_delete_1 = delete_client (cur,5)



        # 7) Вызов функции, позволяющей найти клиента по его данным: имени, фамилии, email или телефону

        # Таблица clients ( для проверки правильности входных данных)
        cur.execute(""" Select * From clients;""")
        print(cur.fetchall())

        # Таблица phones
        cur.execute(""" Select * From phones;""")
        print(cur.fetchall())

        required_client = find_client(cur, 'Anton', None,None, 11122234)
        print(required_client)



conn.close()



