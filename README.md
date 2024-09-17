# Airflow Assignment2 #
### Authorize: Viethq31
Phần 1: Vẽ lại DAG theo ý hiểu trên draw.io và chụp hình paste vào đó.

![Untitled Diagram drawio](https://github.com/user-attachments/assets/aa26652c-1c8c-439a-bc8a-721bf0468dbb)

Phần 2: Mô tả solution.

Các API_KEY, BOT_TOKEN được lưu thành Variables tại 'docker-compose.yaml', qua đó được get() trong file để bảo mật các key quan trọng, sử dụng BASE_CURRENCY để dễ dàng thay 'base' nếu đề bài thay đổi, đặt 'start_date' từ '2024-09-01'.

- Task 1: Sử dụng PythonOperator, sử dụng requests.get để lấy data từ API Forex, và lưu dưới dạng json tại local storage. 

- Task 2: Sử dụng PostgresOperator, connect với Postgres thông qua 'postgres_conn_id' được lưu trong 'docker-compose.yaml' tạo bảng với query 'CREATE TABLE IF NOT EXISTS' để những lần truy vấn sau khi đã tạo bảng sẽ không ảnh hưởng.

- Task 3: Sử dụng PythonOperator, Sử dụng PostgresHook để kết nối với Postgres, tạo df từ file Json, Sử dụng 'cursor.executemany' để insert data từ df vào table exchange_rates thông qua 'upsert_query', sau đó commit() và close() connect.

- Task 4: Sử dụng PythonOperator, lấy toàn bộ data từ bảng thông qua query sql, bắt đầu tính toán độ lệch. Xét từng unique currency, tìm ra được giá trị có độ lệch lớn nhất so với ngày hiện tại, từ giá trị đó tìm dc ngày có giá trị đó. Qua đó append() từng df của từng currency vào df chứa tất cả currency, và lưu file csv về local storage.

- Task 5: Sử dụng PythonOperator, sử dụng Bot_Token và requests.post() để đẩy file csv vào kênh thông báo thông qua chat_id.

- Chạy DAG: Đặt các Operator phù hợp cho các task, đặt 'task_id' và call function để run task
- Cuối cùng xếp thứ tự các task và kiểm tra kết quả tại UI.

- Đối với chạy backfill, tạo 1 dag mới thì airflow sẽ tự chạy backfill kể từ start_date

Phần 3 chụp hình Output của DAG sau khi chạy xong.

![Complete](https://github.com/user-attachments/assets/b4a3f354-f1f7-4a97-80b8-8b9a1dc10608)

Sau khi chạy hoàn thành

![Graph view](https://github.com/user-attachments/assets/2ac3aa7c-c62b-49b3-a3f1-ff823d5cae8a)

Graph view thứ tự các Task 

![Run backfill](https://github.com/user-attachments/assets/0b77f692-b8ef-4a6a-bf69-4ce2fc591fe2)

Chạy Backfill từ ngày 1/9/2024

![Report from bot](https://github.com/user-attachments/assets/443203fc-a282-4226-9434-521748c9ac78)

Report qua bot gửi qua Telegram
