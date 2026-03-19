# CSV Manager (FastAPI + Streamlit)

Приложение для работы с CSV файлами через веб-интерфейс:

- Просмотр данных с пагинацией

- Добавление новых строк с подтверждением

- Удаление строк

- Графики consumption и price через Plotly

- Inline таблица только для удаления и добавления

# Установка


1 Установить зависимости:

`pip install -r requirements.txt
`

2 Запуск FastAPI backend


`cd backend
`

`uvicorn main:app --reload`

3 Запуск Streamlit UI

`cd frontend
`

`streamlit run app.py`
