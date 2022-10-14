### Airflow 환경 설정 변경

1. Airflow의 환경 설정이 들어있는 파일의 이름은?
   ```
   airflow.cfg
2. 이 파일에서 Airflow를 API 형태로 외부에서 조작하고 싶다면 어느 섹션을 변경해야하는가?
   ```
   auth_backend = airflow.api.auth.backend.basic_auth
3. Variable에서 변수의 값이 encrypted가 되려면 변수의 이름에 어떤 단어들이 들어가야 하는데 이 단어들은 무엇일까? :)
   ```
   'password', 'secret', 'passwd', 'authorization', 'api_key', 'apikey', 'access_token'

4. 이 환경 설정 파일이 수정되었다면 이를 실제로 반영하기 위해서 해야 하는 일은?
   ```
   systemctl restart airflow-webserver
   systemctl restart airflow-scheduler

5. DAGs 폴더에 새로운 Dag를 만들면 언제 실제로 Airflow 시스템에서 이를 알게 되나?
   이 스캔주기를 결정해주는 키의 이름이 무엇인가?
   ```
   # 기본 5분 주기로 조회합니다
   dag_dir_list_interval = 300