import requests
import json
import csv
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

results = []
url = "https://api.hh.ru/vacancies"

params = {
    "text": "стажировка студент",
    "area": 40,           # Казахстан
    "experience": "noExperience",
    "per_page": 100,      
    "page": 0,          
    "only_with_salary": False
}

print("Начинаем сбор данных с API hh.ru...")

while True:
    print(f"Загрузка страницы {params['page']}...")
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    
    if resp.status_code != 200:
        print(f"Ошибка API: {resp.status_code} - {resp.text[:300]}")
        break
        
    data = resp.json()
    items = data.get("items", [])
    
    if not items:
        break
        
    for item in items:
        salary_from = None
        salary_to = None
        currency = None
        if item.get("salary"):
            salary_from = item["salary"].get("from")
            salary_to = item["salary"].get("to")
            currency = item["salary"].get("currency")
        
        record = {
            "id": item.get("id"),
            "title": item.get("name", ""),
            "company": item.get("employer", {}).get("name", ""),
            "city": item.get("area", {}).get("name", ""),
            "salary_from": salary_from,
            "salary_to": salary_to,
            "currency": currency,
            "experience": item.get("experience", {}).get("name", ""),
            "employment": item.get("employment", {}).get("name", ""),
            "schedule": item.get("schedule", {}).get("name", ""),
            "published_at": item.get("published_at", "")[:10] if item.get("published_at") else "",
            "url": item.get("alternate_url", ""),
            "has_test": item.get("has_test", False),
            "response_letter_required": item.get("response_letter_required", False)
        }
        results.append(record)
    
    total_pages = data.get("pages", 1)
    if params["page"] >= total_pages - 1:
        break
        
    params["page"] += 1
    time.sleep(0.5)

print(f"\nГотово! Всего собрано записей: {len(results)}")


if results:
    with open("vacancies.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    print("✅ Данные сохранены в vacancies.json")

    keys = results[0].keys()
    with open("vacancies.csv", "w", encoding="utf-8", newline="") as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(results)
    print("✅ Данные сохранены в vacancies.csv")
else:
    print("Нет данных для сохранения.")