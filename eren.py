import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from tradingview_screener import Query, Column
from datetime import datetime, time as dtime
import time
import threading

# Google Sheets API kimlik doğrulama
SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)

SPREADSHEET_ID = '1fx3YUwtSNvx3xlr2RUDpa_Ml8F831_1YBBLB18OFHcI'  # Google Sheets belgenizin ID'si
SHEET_NAME = 'SAFAKHOMETECHNICAL'  # Çalışma sayfanızın adı

# Pazarlara ait hisseler
ANAPAZAR = [
    "ACSEL", "AGYO", "AKENR", "AKSUE", "AKYHO", "ALKA", "ALKLC", "ALMAD", 
    "ANELE", "ANGEN", "ARSAN", "ARTMS", "ARZUM", "ATAGY", "ATEKS", "AVGYO", 
    "AVHOL", "AVOD", "AYCES", "AYEN", "BAKAB", "BANVT", "BARMA", "BAYRK", 
    "BEGYO", "BEYAZ", "BLCYT", "BMSCH", "BMSTL", "BNTAS", "BOSSA", "BRKSN", 
    "BURCE", "BURVA", "BVSAN", "BYDNR", "CELHA", "CEOEM", "CONSE", "COSMO", 
    "CRDFA", "CUSAN", "DAGI", "DARDL", "DENGE", "DERHL", "DERIM", "DESPC", 
    "DGATE", "DITAS", "DMRGD", "DMSAS", "DNISI", "DOBUR", "DOCO", "DOFER", 
    "DOGUB", "DOKTA", "DURDO", "DYOBY", "DZGYO", "EDATA", "EDIP", "EGEPO", 
    "EGSER", "ENSRI", "EPLAS", "ERSU", "ESEN", "ETILR", "EYGYO", "FADE", 
    "FLAP", "FMIZP", "FONET", "FORMT", "FRIGO", "GEDZA", "GENTS", "GLBMD", 
    "GLRYH", "GMTAS", "GSDDE", "GZNMI", "HATEK", "HDFGS", "HKTM", "HUBVC", 
    "HURGZ", "ICBCT", "ICUGS", "IHEVA", "IHGZT", "IHLAS", "IHLGM", "IHYAY", 
    "INGRM", "INTEM", "ISGSY", "ISKPL", "IZFAS", "IZINV", "KAPLM", "KARYE", 
    "KFEIN", "KGYO", "KIMMR", "KLMSN", "KLSYN", "KNFRT", "KRGYO", "KRONT", 
    "KRPLS", "KRSTL", "KRTEK", "KUTPO", "LIDFA", "LINK", "LKMNH", "LUKSK", 
    "MACKO", "MAKIM", "MAKTK", "MANAS", "MARBL", "MARTI", "MEGAP", "MEKAG", 
    "MEPET", "MERCN", "MERIT", "MERKO", "METRO", "METUR", "MHRGY", "MNDRS", 
    "MNDTR", "MRSHL", "MSGYO", "MTRKS", "MZHLD", "NIBAS", "NUGYO", "OBASE", 
    "OFSYM", "ONCSM", "ONRYT", "ORCAY", "OSMEN", "OSTIM", "OYLUM", "OZGYO", 
    "OZRDN", "OZSUB", "OZYSR", "PAGYO", "PAMEL", "PCILT", "PENGD", "PINSU", 
    "PKART", "PKENT", "PLTUR", "PRDGS", "PRKAB", "PRKME", "PRZMA", "PSDTC", 
    "RNPOL", "RODRG", "RTALB", "RUBNS", "SAFKR", "SAMAT", "SANFM", "SANKO", 
    "SEGYO", "SEKFK", "SEKUR", "SELVA", "SEYKM", "SILVR", "SKTAS", "SKYLP", 
    "SKYMD", "SMART", "TDGYO", "TEKTU", "TERA", "TGSAS", "TLMAN", "TMPOL", 
    "TRILC", "TUCLK", "TURGG", "ULUFA", "ULUSE", "UNLU", "VANGD", "VERTU", 
    "VKING", "YAYLA", "YESIL", "YGYO", "YKSLN", "YYAPI", "ZEDUR"
]
YILDIZPAZAR = [
    "A1CAP", "ADEL", "ADESE", "ADGYO", "AEFES", "AFYON", "AGESA", "AGHOL", 
    "AGROT", "AHGAZ", "AKBNK", "AKCNS", "AKFGY", "AKFYE", "AKGRT", "AKSA", 
    "AKSEN", "AKSGY", "ALARK", "ALBRK", "ALCAR", "ALCTL", "ALFAS", "ALGYO", 
    "ALKIM", "ALTNY", "ALVES", "ANHYT", "ANSGR", "ARASE", "ARCLK", "ARDYZ", 
    "ARENA", "ASELS", "ASGYO", "ASTOR", "ASUZU", "ATAKP", "ATATP", "AVPGY", 
    "AYDEM", "AYGAZ", "AZTEK", "BAGFS", "BASGZ", "BERA", "BFREN", "BIENY", 
    "BIGCH", "BIMAS", "BINHO", "BIOEN", "BIZIM", "BOBET", "BORLS", "BORSK", 
    "BRISA", "BRKVY", "BRLSM", "BRSAN", "BRYAT", "BSOKE", "BTCIM", "BUCIM", 
    "CANTE", "CATES", "CCOLA", "CEMAS", "CEMTS", "CIMSA", "CLEBI", "CMBTN", 
    "CRFSA", "CVKMD", "CWENE", "DAPGM", "DESA", "DEVA", "DGNMO", "DOAS", 
    "DOHOL", "EBEBK", "ECILC", "ECZYT", "EFORC", "EGEEN", "EGGUB", "EGPRO", 
    "EKGYO", "EKOS", "EKSUN", "ELITE", "EMKEL", "ENERY", "ENJSA", "ENKAI", 
    "ENTRA", "ERBOS", "ERCB", "EREGL", "ESCAR", "ESCOM", "EUPWR", "EUREN", 
    "FENER", "FORTE", "FROTO", "FZLGY", "GARAN", "GEDIK", "GENIL", "GEREL", 
    "GESAN", "GIPTA", "GLCVY", "GLYHO", "GOKNR", "GOLTS", "GOODY", "GOZDE", 
    "GRSEL", "GRTRK", "GSDHO", "GSRAY", "GUBRF", "GWIND", "HALKB", "HATSN", 
    "HEDEF", "HEKTS", "HLGYO", "HOROZ", "HRKET", "HTTBT", "HUNER", "IEYHO", 
    "IHAAS", "IMASM", "INDES", "INFO", "INVEO", "INVES", "IPEKE", "ISCTR", 
    "ISDMR", "ISFIN", "ISGYO", "ISMEN", "ISSEN", "IZENR", "IZMDC", "JANTS", 
    "KAREL", "KARSN", "KARTN", "KATMR", "KAYSE", "KBORU", "KCAER", "KCHOL", 
    "KERVT", "KLGYO", "KLKIM", "KLRHO", "KLSER", "KMPUR", "KOCMT", "KONKA", 
    "KONTR", "KONYA", "KOPOL", "KORDS", "KOTON", "KOZAA", "KOZAL", "KRDMA", 
    "KRDMB", "KRDMD", "KRVGD", "KTLEV", "KTSKR", "KUYAS", "KZBGY", "KZGYO", 
    "LIDER", "LILAK", "LMKDC", "LOGO", "LRSHO", "MAALT", "MAGEN", "MAVI", 
    "MEDTR", "MEGMT", "MGROS", "MIATK", "MIPAZ", "MOBTL", "MOGAN", "MPARK", 
    "MRGYO", "NATEN", "NETAS", "NTGAZ", "NTHOL", "NUHCM", "OBAMS", "ODAS", 
    "ODINE", "ORGE", "OTKAR", "OYAKC", "OYYAT", "OZKGY", "PAPIL", "PARSN", 
    "PASEU", "PATEK", "PEHOL", "PEKGY", "PENTA", "PETKM", "PETUN", "PGSUS", 
    "PNLSN", "PNSUT", "POLHO", "POLTK", "PSGYO", "QUAGR", "RALYH", "RAYSG", 
    "REEDR", "RGYAS", "RYGYO", "RYSAS", "SAHOL", "SARKY", "SASA", "SAYAS", 
    "SDTTR", "SEGMN", "SELEC", "SISE", "SKBNK", "SMRTG", "SNGYO", "SNICA", 
    "SOKE", "SOKM", "SRVGY", "SUNTK", "SURGY", "SUWEN", "TABGD", "TARKM", 
    "TATEN", "TATGD", "TAVHL", "TCELL", "TEZOL", "THYAO", "TKFEN", "TKNSA", 
    "TMSN", "TNZTP", "TOASO", "TRCAS", "TRGYO", "TSK", "TSKB", "TSPOR", 
    "TTKOM", "TTRAK", "TUKAS", "TUPRS", "TUREX", "TURSG", "ULKER", "ULUUN", 
    "USAK", "VAKBN", "VAKFN", "VAKKO", "VBTYZ", "VERUS", "VESBE", "VESTL", 
    "VKGYO", "VRGYO", "YAPRK", "YATAS", "YEOTK", "YGGYO", "YIGIT", "YKBNK", 
    "YUNSA", "YYLGD", "ZOREN", "ZRGYO"
]

# Tarama fonksiyonları
def Sahmeran():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('ADX+DI') > Column('ADX-DI'),
                    Column('MACD.macd').crosses_above(Column('MACD.signal')),
                    Column('MoneyFlow') > 50.0,
                    Column('HullMA9') < Column('close'),
                    Column('VWAP') < Column('close'),
                    Column('VWMA') < Column('close'),
                    Column('Ichimoku.CLine') > Column('Ichimoku.BLine')
                    )
        .get_scanner_data())[1]
    return Tarama

def Vira():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('SMA10') > Column('SMA50'),
                    Column('SMA200') < Column('SMA50'),
                    Column('Aroon.Up') >= 100,
                    Column('Aroon.Down') < Column('Aroon.Up'),
                    Column('ChaikinMoneyFlow') > 0,
                    Column('HullMA9') < Column('close'),
                    Column('Ichimoku.BLine') < Column('Ichimoku.CLine')
                    )
        .get_scanner_data())[1]
    return Tarama

def Longcu():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('volume') > 1E6,
                    Column('RSI').between(45,70),
                    Column('ADX') > 15,
                    Column('ADX+DI') > Column('ADX-DI'),
                    Column('Mom') > 0.0,
                    Column('MACD.macd') > -0.1,
                    Column('MACD.signal').crosses_below(Column('MACD.macd')),
                    Column('P.SAR') < Column('close'),
                    Column('MoneyFlow') > 45.0,
                    Column('Ichimoku.CLine') < Column('close'),
                    Column('Ichimoku.BLine') < Column('close'),
                    )
        .get_scanner_data())[1]
    return Tarama



# Verileri güncelleme fonksiyonu
def update_data():
    # Tarama fonksiyonlarını çağırın
    Sahmeran_df = Sahmeran()
    Vira_df = Vira()
    Longcu_df = Longcu() 

    # Boş veya tümü NA olan DataFrame'leri hariç tut
    tarama_list = [df for df in [Sahmeran_df, Vira_df, Longcu_df] if not df.empty]

    # Taramaları birleştirin ve etiketleyin
    tarama_dict = {
        'Şahmeran': Sahmeran_df, 'Vira': Vira_df, 'Longcu': Longcu_df
    }

    for name, df in tarama_dict.items():
        if not df.empty:
            df['Taramalar'] = name

    combined_df = pd.concat(tarama_list, ignore_index=True)

    # Hisse senedi hangi pazarda olduğunu belirleyin
    def determine_market(ticker):
        if ticker in ANAPAZAR:
            return "ANA"
        elif ticker in YILDIZPAZAR:
            return "YILDIZ"
        else:
            return "DİĞER"

    combined_df['Pazar'] = combined_df['name'].apply(determine_market)

    # Verileri gruplandırın ve düzenleyin
    combined_df = combined_df.groupby(['ticker', 'name', 'change', 'close', 'volume', 'Perf.W', 'Pazar'], as_index=False).agg({'Taramalar': ' - '.join})
    combined_df['close'] = round(combined_df['close'], 2)
    combined_df['Perf.W'] = round(combined_df['Perf.W'], 2)
    combined_df['Tarama Sayısı'] = combined_df['Taramalar'].str.count('|'.join(tarama_dict.keys()))
    combined_df['change'] = round(combined_df['change'], 2)
    combined_df_2 = combined_df.sort_values(by='Tarama Sayısı', ascending=False).reset_index(drop=True)

    # Güncelleme zamanını belirlenen formatta ekleyin
    update_time = datetime.now().strftime('%d %B %H:%M')
    update_time = update_time.lstrip('0').replace("January", "Ocak").replace("February", "Şubat").replace("March", "Mart").replace("April", "Nisan").replace("May", "Mayıs").replace("June", "Haziran").replace("July", "Temmuz").replace("August", "Ağustos").replace("September", "Eylül").replace("October", "Ekim").replace("November", "Kasım").replace("December", "Aralık")
    combined_df_2['Güncelleme Zamanı'] = update_time

    # Sütunların sırasını güncelleyin
    columns_order = ['ticker', 'name', 'change', 'close', 'volume', 'Perf.W', 'Tarama Sayısı', 'Taramalar', 'Pazar', 'Güncelleme Zamanı']
    combined_df_2 = combined_df_2[columns_order]

    # Sütun isimlerini güncelleyin
    combined_df_2.columns = ['Kod', 'Hisse', 'Günlük Değişim %', 'Fiyat', 'Hacim', 'Perf.W', 'Tarama Sayısı', 'Taramalar', 'Pazar', 'Güncelleme Zamanı']

    # DataFrame'i Google Sheets'e aktarmak için hazırlayın
    values = [combined_df_2.columns.tolist()] + combined_df_2.values.tolist()

    # Verileri Google Sheets'e yazmak için gövde oluşturun
    body = {
        'values': values
    }

    # Sayfayı temizle
    clear_request = service.spreadsheets().values().clear(
        spreadsheetId="1fx3YUwtSNvx3xlr2RUDpa_Ml8F831_1YBBLB18OFHcI",
        range=f'{"SAFAKHOMETECHNICAL"}!A:Z'
    )
    clear_request.execute()

    # Verileri Google Sheets'e yazın
    result = service.spreadsheets().values().update(
        spreadsheetId="1fx3YUwtSNvx3xlr2RUDpa_Ml8F831_1YBBLB18OFHcI",
        range=f'{"SAFAKHOMETECHNICAL"}!A1',
        valueInputOption='RAW',
        body=body
    ).execute()

    print(f"{result.get('updatedCells')} Hücre güncellendi (Günlük Tarama). Güncelleme saati: {update_time}")

# Kullanıcı komutlarını kontrol etme fonksiyonu
def check_user_input():
    while True:
        user_input = input().strip().lower()
        if user_input == "yenile":
            update_data()

# Otomatik yenileme fonksiyonu
def auto_update():
    while True:
        now = datetime.now()
        current_time = now.time()
        # Hafta içi 5 gün çalışır ve saat 9-20 arası çalışır
        if now.weekday() < 5 and dtime(9, 0) <= current_time <= dtime(20, 0):
            update_data()
        time.sleep(60)

# İki işlevi paralel olarak çalıştır
input_thread = threading.Thread(target=check_user_input)
update_thread = threading.Thread(target=auto_update)

input_thread.start()
update_thread.start()

input_thread.join()
update_thread.join()
