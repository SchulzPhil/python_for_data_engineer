from datetime import datetime
from pathlib import Path
from io import StringIO
from csv import writer
from functools import lru_cache

from csv import DictReader
from uuid import uuid4

from fastapi import FastAPI, UploadFile, Query, HTTPException
from pydantic import BaseModel
from aiofiles import open as a_open


UPLOAD_DIR = Path('files')
UPLOAD_DIR.mkdir(exist_ok=True)


app = FastAPI()


class CSVRow(BaseModel):
    timestep: datetime
    consumption_eur: int
    consumption_sib: int
    price_eur: float
    price_sib: float

    def to_csv_line(self):
        buffer = StringIO()
        writer_ = writer(buffer, lineterminator='\n')
        writer_.writerow([
            self.timestep.strftime('%Y-%m-%d %H:%M'),
            self.consumption_eur,
            self.consumption_sib,
            self.price_eur,
            self.price_sib,
        ])
        return buffer.getvalue()


@lru_cache(maxsize=32)
def count_lines(file_path: str) -> int:
    path = Path(file_path)

    with path.open('r', encoding='utf-8') as f:
        return max(sum(1 for _ in f) - 1, 0)


@app.post('/load_csv')
async def load_csv(csv: UploadFile):
    content = await csv.read()
    file_name = uuid4()
    file_path = UPLOAD_DIR / f'{file_name}.csv'

    async with a_open(file_path, 'wb') as f:
        await f.write(content)

    return {'message': f'your file {file_name}'}


@app.get('/records/{filename}')
async def get_records(
    filename: str,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000)
):
    if not filename.endswith('.csv'):
        raise HTTPException(400, 'Only CSV allowed')

    file_path = (UPLOAD_DIR / filename).resolve()

    if not file_path.exists():
        raise HTTPException(404, 'File not found')

    start = (page - 1) * limit
    end = start + limit

    records = []

    total = count_lines(str(file_path))

    async with a_open(file_path, 'r', encoding='utf-8') as f:
        header = await f.readline()

        reader = DictReader([header])
        fieldnames = reader.fieldnames

        if not fieldnames:
            raise HTTPException(400, 'Invalid CSV header')

        idx = 0

        async for line in f:

            if idx >= end:
                break

            if idx >= start:
                row = next(DictReader([line], fieldnames=fieldnames))
                row['id'] = idx + 1
                records.append(row)

            idx += 1

    return {
        'page': page,
        'limit': limit,
        'total': total,
        'pages': (total + limit - 1) // limit,
        'data': records,
    }


@app.delete('/records/{filename}/{row_id}')
async def delete_row(filename: str, row_id: int):

    file_path = (UPLOAD_DIR / filename).resolve()

    if not file_path.exists():
        raise HTTPException(404, 'File not found')

    temp_path = file_path.with_suffix('.tmp')

    async with a_open(file_path, 'r', encoding='utf-8') as src, \
               a_open(temp_path, 'w', encoding='utf-8') as dst:

        header = await src.readline()
        await dst.write(header)

        idx = 1

        async for line in src:
            if idx != row_id:
                await dst.write(line)
            idx += 1

    temp_path.replace(file_path)

    count_lines.cache_clear()

    return {'status': 'deleted', 'row_id': row_id}


@app.post('/records/{filename}')
async def append_row(filename: str, row: CSVRow):

    file_path = (UPLOAD_DIR / filename).resolve()

    existed_before = file_path.exists()

    header = ','.join([
        'timestep',
        'consumption_eur',
        'consumption_sib',
        'price_eur',
        'price_sib',
    ]) + '\n'

    line = f'{row.timestep.isoformat()},{row.consumption_eur},{row.consumption_sib},{row.price_eur},{row.price_sib}\n'

    async with a_open(file_path, 'a', encoding='utf-8') as f:
        if not existed_before:
            await f.write(header)
        await f.write(line)

    count_lines.cache_clear()

    return {'status': 'added'}
