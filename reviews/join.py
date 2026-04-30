import json
import os

def merge_json_files():
    # Список имен твоих файлов
    files_to_merge = [
        'dvjob_google_search_threads.json',
        'enbek_insta_threas.json',
        'hh_insta_threads.json'
    ]
    
    output_file = 'all_reviews.json'
    all_reviews = []

    print("Начинаю объединение файлов...")

    for file_path in files_to_merge:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    # Проверяем, что в файле список
                    if isinstance(data, list):
                        all_reviews.extend(data)
                        print(f"✅ Добавлено {len(data)} записей из {file_path}")
                except Exception as e:
                    print(f"❌ Ошибка в файле {file_path}: {e}")
        else:
            print(f"⚠️ Файл не найден: {file_path}")

    # Сохраняем результат
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_reviews, f, ensure_ascii=False, indent=2)

    print(f"\n🚀 Готово! Все отзывы (всего {len(all_reviews)}) сохранены в {output_file}")

if __name__ == "__main__":
    merge_json_files()