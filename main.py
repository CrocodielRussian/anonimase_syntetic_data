import pandas as pd
import json
import re

def mask(df, key, value):
    return df[df[key] != value]

def counting_of_unique_rows_mask(df, key, value):
    return df[df[key] == value]

def mask_cc_number(cc_string, mask_char='*'):
   digits_to_mask = sum(map(str.isdigit, cc_string))
   masked_cc_string = re.sub('\d', mask_char, cc_string, digits_to_mask)

   return masked_cc_string

def data_anonymization(df):
    departure_times = df["Время отъезда"].tolist()
    arrived_times = df["Время приезда"].tolist()
    start_cities = df["Откуда"].tolist()
    end_cities = df["Куда"].tolist()
    passports = df["Паспорт"].tolist()
    new_passports = [""] * len(passports)
    trips = df["Рейс"].tolist()
    seats = df["Место"].tolist()

    title_trips = []
    for trip in trips:
        if not trip in title_trips:
            title_trips.append(trip)
    
    inx = 0
    ln_forward = 0
    ln_backward = 0
    first_arrived_time_forward = 0
    last_arrived_time_forward = 0
    first_arrived_time_backward = 0
    last_arrived_time_backward = 0

    first_departure_time_forward = 0
    last_departure_time_forward = 0
    first_departure_time_backward = 0
    last_departure_time_backward = 0

    new_arrived_times = []
    new_departure_times = []
    cnt = 0
    for i in range(0, len(trips)):
        if inx >= len(title_trips):
            print("end")
            break
        if trips[i] == title_trips[inx + 1]:
            if first_departure_time_backward == 0:
                first_departure_time_backward = departure_times[i]
                first_arrived_time_backward = arrived_times[i]
            ln_backward += 1
            last_departure_time_backward = departure_times[i]
            last_arrived_time_backward = arrived_times[i]
        elif trips[i] == title_trips[inx]:
            if first_departure_time_forward == 0:
                first_departure_time_forward = departure_times[i]
                first_arrived_time_forward= arrived_times[i]
            ln_forward += 1
            last_departure_time_forward = departure_times[i]
            last_arrived_time_forward = arrived_times[i]
        else:
            for _ in range(ln_forward):
                new_departure_times.append(f"{first_departure_time_forward}-{last_departure_time_forward}")
                new_arrived_times.append(f"{first_arrived_time_forward}-{last_arrived_time_forward}")
            for _ in range(ln_backward):
                new_departure_times.append(f"{first_departure_time_backward}-{last_departure_time_backward}")
                new_arrived_times.append(f"{first_arrived_time_backward}-{last_arrived_time_backward}")

            inx += 2
            cnt += ln_forward + ln_backward
            ln_forward = 1
            ln_backward = 0
            first_departure_time_forward = departure_times[i]
            first_departure_time_backward = 0

    cnt += ln_forward + ln_backward
    for _ in range(ln_forward):
        new_departure_times.append(f"{first_departure_time_forward}-{last_departure_time_forward}")
        new_arrived_times.append(f"{first_arrived_time_forward}-{last_arrived_time_forward}")
    for _ in range(ln_backward):
        new_departure_times.append(f"{first_departure_time_backward}-{last_departure_time_backward}")
        new_arrived_times.append(f"{first_arrived_time_backward}-{last_arrived_time_backward}")

    departure_times = df["Время отъезда"].tolist()
    trips = df["Рейс"].tolist()
    new_seats = []
    for seat in seats:
        wagon, place = seat.split("-")
        new_seats.append(wagon)

    cards = df["Карта оплаты"].tolist()
    new_cards = []
    for card in cards:
        card = card[:7] + mask_cc_number(card[7:])
        new_cards.append(card)

    templates = ""
    with open('datasets/names.json') as f:
        templates = json.load(f) 
    female_surnames = set(templates["female_surnames"])

    names = df["ФИО"].tolist()
    new_names = []
    for name in names:
        first_name, middle_name, last_name = name.split()
        masked_person = ""
        if(first_name in female_surnames):
            new_names.append("Ж")
        else:
            new_names.append("М")
    
    costs = df["Цена билета"].tolist()
    new_df = pd.DataFrame.from_dict({
        "ФИО" : new_names,
        "Паспорт" : new_passports,
        "Рейс" : trips,
        "Место" : new_seats,
        "Откуда" : trips,
        "Куда" : trips,
        "Время отъезда" : new_departure_times,
        "Время приезда" : new_arrived_times,
        "Цена билета" : costs,
        "Карта оплаты" : new_cards
    })

    duplicate = new_df.groupby(new_df.columns.tolist(), as_index=False).size()
    duplicate = duplicate.reset_index()  # make sure indexes pair with number of rows

    
    pd.DataFrame.mask = counting_of_unique_rows_mask
    k_anonimity = min(duplicate["size"])

    pd.DataFrame.mask = counting_of_unique_rows_mask
    print("Топ-5 худших k-анонимити до их удаления")
    k = 0
    while k < 5:
        if k_anonimity > len_dataset:
            break
        res = duplicate.mask('size', k_anonimity).shape[0] * k_anonimity
        if res != 0:
            print(f"k-анонимити {k_anonimity}: {round(res / (len_dataset / 100), 3)}")
            k += 1
        k_anonimity += 1

    pd.DataFrame.mask = mask
    if len(trips) <= 51000:
        for k in range(1, 10):
            duplicate = duplicate.mask('size', k)
    elif len(trips) <= 105000:
        for k in range(1, 7):
            duplicate = duplicate.mask('size', k)
    else:
        for k in range(1, 5):
            duplicate = duplicate.mask('size', k)

        pd.DataFrame.mask = counting_of_unique_rows_mask

    print("Топ-5 худших k-анонимити после удаления наихудших")
    k_anonimity = min(duplicate["size"])
    pd.DataFrame.mask = counting_of_unique_rows_mask

    k = 0
    while k < 5:
        if k_anonimity > len_dataset:
            break
        res = duplicate.mask('size', k_anonimity).shape[0] * k_anonimity
        if res != 0:
            print(f"k-анонимити {k_anonimity}: {round(res / (len_dataset / 100), 3)}")
            k += 1
        k_anonimity += 1


    return duplicate

if __name__ == "__main__":
    read = pd.read_excel('output.xlsx') 
    df = pd.DataFrame(read)

    len_dataset = df.shape[0]

    choose = int(input("Что вы хотите выполнить: 1 - обезличивание, 2 - подсчёт k-анонимити входного файла: "))
    
    if choose == 2:
        quasi_ident = set(map(int, input("Какие квази-идентификаторы вы хотите выбрать(введите номера идентификаторов, которые вы выбрали, через пробел): 1- Рейс, 2 - Место, 3 - Откуда, 4 - Куда, 5 - Время отъезда, 6 - Время приезда, 7 - Цена билета, 8 - Карта оплаты: ").split()))

        idents = ["Рейс", "Место", "Откуда", "Куда", "Время отъезда", "Время приезда", "Цена билета", "Карта оплаты"]
        
        delete_columns = []
        for i in range(1, len(idents) + 1):
            if i not in quasi_ident:
                delete_columns.append(idents[i-1])
        
        new_df = df.drop(delete_columns, axis=1)

        duplicate = new_df.groupby(new_df.columns.tolist(), as_index=False).size()

        pd.DataFrame.mask = counting_of_unique_rows_mask
        k_anonimity = min(duplicate["size"])

        pd.DataFrame.mask = counting_of_unique_rows_mask

        k = 0
        while k < 5:
            if k_anonimity > len_dataset:
                break
            res = duplicate.mask('size', k_anonimity).shape[0] * k_anonimity
            if res != 0:
                print(f"k-анонимити {k_anonimity}: {round(res / (len_dataset / 100), 3)}%")
                k += 1
            k_anonimity += 1

    elif choose == 1:
        print(f"Начальный размер датасета - {len_dataset}")
        new_df = data_anonymization(df)
        
        new_df = new_df.loc[new_df.index.repeat(new_df['size'])]
        print(f"Конечный размер датасета - {new_df.shape[0]}")
        new_df = new_df.drop(["size"], axis = 1)
        new_df.to_excel("example.xlsx", index=False)