# postgreSQL_connect
* 將連線資訊儲存在 *name*.env 中
* 藉由 dbConInfo 中的 super().__init__(env_name) 觸發 read_dotenv 將連線資訊讀取至environ
* 使用 env.host() 等方法，從 os.environ 讀取需要連線資訊
* 使用 python 裝飾器，快速引入 *name*.env 中的連線資訊
* 使用 python class 中的 __ enter __ 與 __ exit __ 方法，連線取得資訊後就關閉連線

## 使用方式：
```python=
@dbConInfo_deco('line1_info') ＃此處為 env 名稱，修改此處即可針對不同資料庫存取資訊
def select_db(dci):
    dci_result = None
    sql = f'''
        Select * From public.record
        WHERE lot_id='cccc'
        AND time_folder='eeee' 
        AND path_serial='yyyy'
    '''
    dci_result = dci.select(sql)
    return dci_result

result=select_db()
print(result)
```