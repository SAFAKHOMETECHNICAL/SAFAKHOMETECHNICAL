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
SHEET_NAME = 'SAFAKHOMETECHNICAL' # Çalışma sayfanızın adı

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

def Burada41():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('volume') > 1E6,
                    Column('close') > Column('open'),
                    Column('RSI').between(45,65),
                    Column('RSI7') < 70,
                    Column('ChaikinMoneyFlow') > 0,
                    Column('HullMA9') < Column('close'),
                    Column('Stoch.RSI.K') >= (Column('Stoch.RSI.D')),
                    Column('Ichimoku.CLine') < Column('close')
                    )
        .get_scanner_data())[1]
    return Tarama

def DayTrade():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('relative_volume_10d_calc') > 1.4,
                    Column('RSI') > 50,
                    Column('ADX') > 25,
                    Column('ADX+DI') > Column('ADX-DI'),
                    Column('EMA20') < Column('close'),
                    Column('EMA50') < Column('close'),
                    Column('EMA200') < Column('close'),
                    Column('MACD.macd') > Column('MACD.signal'),
                    Column('P.SAR') < Column('close'),
                    Column('MoneyFlow') > 50.0,
                    Column('ChaikinMoneyFlow') > 0.2,
                    Column('HullMA9') < Column('close'),
                    Column('Stoch.RSI.K') >= (Column('Stoch.RSI.D')),
                    Column('Ichimoku.CLine') < Column('close')
                    )
        .get_scanner_data())[1]
    return Tarama

def FridaKahlo():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('volume') > 1E6,
                    Column('relative_volume_10d_calc') > 1.618,
                    Column('close') > Column('open'),
                    Column('RSI').between(45,65),
                    Column('Mom') > 0.0,
                    Column('EMA50') < Column('close'),
                    Column('EMA200') < Column('close'),
                    Column('SMA20') < Column('close'),
                    Column('MACD.macd') > Column('MACD.signal'),
                    Column('P.SAR') < Column('close'),
                    Column('ChaikinMoneyFlow').between(-0.2,0.3),
                    Column('Stoch.RSI.K') > (Column('Stoch.RSI.D')),
                    )
        .get_scanner_data())[1]
    return Tarama

def HodriMeydan():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('volume') > 2E5,
                    Column('close') > Column('open'),
                    Column('RSI').between(45,65),
                    Column('RSI7').between(40,65),
                    Column('SMA10') > Column('SMA100'),
                    Column('ChaikinMoneyFlow') > 0,
                    Column('HullMA9') > Column('low'),
                    Column('Stoch.RSI.K') >= (Column('Stoch.RSI.D')),
                    )
        .get_scanner_data())[1]
    return Tarama

def Pasa():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('close') > Column('open'),
                    Column('ADX') > 20,
                    Column('EMA5').crosses_above(Column('SMA10')),
                    Column('EMA10') < Column('close'),
                    Column('EMA20') < Column('close'),
                    Column('EMA50') < Column('close'),
                    Column('EMA100') < Column('close'),
                    Column('EMA200') < Column('close'),
                    Column('HullMA9') < Column('close'),
                    )
        .get_scanner_data())[1]
    return Tarama

def Ekmek():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('relative_volume_10d_calc') > 1.4,
                    Column('RSI') > 50,
                    Column('ADX') > 25,
                    Column('ADX+DI') > Column('ADX-DI'),
                    Column('EMA20') < Column('close'),
                    Column('EMA50') < Column('close'),
                    Column('EMA200') < Column('close'),
                    Column('MACD.macd') > Column('MACD.signal'),
                    Column('BB.upper').crosses_below(Column('close')),
                    Column('P.SAR') < Column('close'),
                    Column('MoneyFlow') > 50.0,
                    Column('HullMA9') < Column('close'),
                    Column('Stoch.RSI.K') > (Column('Stoch.RSI.D')),
                    Column('Ichimoku.CLine') < Column('close')
                    )
        .get_scanner_data())[1]
    return Tarama

def Flz():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('volume') > 5E4,
                    Column('close') > Column('open'),
                    Column('RSI') > 55,
                    Column('ADX').between(25,50),
                    Column('ADX+DI') > 20,
                    Column('ADX-DI') < Column('ADX+DI'),
                    Column('Mom') > 0.0,
                    Column('SMA20') < Column('close'),
                    Column('Aroon.Up') >= 100,
                    Column('Aroon.Down') < Column('Aroon.Up'),
                    Column('P.SAR') < Column('close'),
                    Column('MoneyFlow') > 50.0,
                    Column('ChaikinMoneyFlow') > 0.2,
                    Column('Stoch.RSI.K') > (Column('Stoch.RSI.D')),
                    Column('Ichimoku.CLine') < Column('close'),
                    Column('Ichimoku.BLine') < Column('close'),
                    )
        .get_scanner_data())[1]
    return Tarama

def Macd():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('MACD.macd') > -0.1,
                    Column('MACD.signal').crosses_below(Column('MACD.macd')),
                    Column('P.SAR') < Column('close'),
                    )
        .get_scanner_data())[1]
    return Tarama

def Alp():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('relative_volume_10d_calc') > 1.618,
                    Column('close').crosses_above('EMA5'),
                    Column('MACD.macd') > Column('MACD.signal'),
                    Column('BB.lower') <= Column('low'),
                    Column('Stoch.RSI.K') > (Column('Stoch.RSI.D')),
                    )
        .get_scanner_data())[1]
    return Tarama

def AdınıSenKoy():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('relative_volume_10d_calc') > 1.2,
                    Column('close') > Column('open'),
                    Column('RSI').between(30,80),
                    Column('ADX+DI') > Column('ADX-DI'),
                    Column('Mom') > -0.1,
                    Column('EMA20') < Column('close'),
                    Column('EMA50') < Column('close'),
                    Column('EMA200') < Column('close'),
                    Column('Aroon.Up').between(70,100),
                    Column('Aroon.Down') < Column('Aroon.Up'),
                    Column('P.SAR') < Column('close'),
                    Column('MoneyFlow') > 40.0,
                    Column('VWAP') < Column('close'),
                    Column('VWMA') < Column('close'),
                    Column('Ichimoku.CLine') < Column('close'),
                    Column('Ichimoku.BLine') < Column('close'),
                    Column('Ichimoku.Lead2').crosses_below('close'),
                    )
        .get_scanner_data())[1]
    return Tarama

def DuseniKiran():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('average_volume_10d_calc') > 75E4,
                    Column('P.SAR').crosses_below('close'),
                    Column('Value.Traded') > 5E5,
                    Column('CCI20') > 90,
                    )
        .get_scanner_data())[1]
    return Tarama

def AtilganDuseniKiran():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('average_volume_10d_calc') > 75E4,
                    Column('relative_volume_10d_calc') >= 1.2,
                    Column('RSI').between(30,80),
                    Column('Mom') > 0,
                    Column('P.SAR') < Column('close'),
                    Column('Value.Traded') > 5E5,
                    Column('ChaikinMoneyFlow').between(-0.2,0.3),
                    Column('CCI20') > 90,
                    Column('VWMA').crosses_below(Column('close')),
                    )
        .get_scanner_data())[1]
    return Tarama

def DuseniKiranYeni():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('RSI') > 55,
                    Column('EMA5') < Column('close'),
                    Column('EMA20') < Column('EMA5'),
                    Column('EMA50') < Column('EMA20'),
                    Column('CCI20').crosses_above(100),
                    )
        .get_scanner_data())[1]
    return Tarama

def Tiga():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('relative_volume_10d_calc') > 1.618,
                    Column('close') > Column('open'),
                    Column('ADX') > 13,
                    Column('ADX+DI') > Column('ADX-DI'),
                    Column('Mom') > 0.236,
                    Column('MACD.macd') > Column('MACD.signal'),
                    Column('MACD.signal') > -0.618,
                    Column('P.SAR') < Column('close'),
                    Column('MoneyFlow') > 55.0,
                    Column('ChaikinMoneyFlow').between(-0.236,0.236),
                    Column('Stoch.RSI.K') > (Column('Stoch.RSI.D')),
                    Column('Ichimoku.CLine') > Column('Ichimoku.BLine'),
                    )
        .get_scanner_data())[1]
    return Tarama

def RsiZkn():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('average_volume_10d_calc') > 5E5,
                    Column('RSI').crosses_above(30),
                    Column('ADX') > 14,
                    Column('Mom') > -10,
                    Column('Stoch.RSI.D') < 40,
                    )
        .get_scanner_data())[1]
    return Tarama

def Oz():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('relative_volume_10d_calc') > 1.4,
                    Column('RSI') > 70,
                    Column('ADX+DI') > Column('ADX-DI'),
                    Column('P.SAR') < Column('close'),
                    Column('MoneyFlow') > 50.0,
                    Column('ChaikinMoneyFlow') > 0,
                    Column('Stoch.RSI.K') > (Column('Stoch.RSI.D')),
                    Column('VWAP') < Column('close'),
                    Column('VWMA') < Column('close'),
                    Column('Ichimoku.CLine') < Column('close'),
                    )
        .get_scanner_data())[1]
    return Tarama

def RsiDip():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('RSI').crosses_above(30),
                    Column('Mom') > -10,
                    Column('Stoch.RSI.D') < 40,
                    Column('volume') > Column('average_volume_10d_calc'),
                    Column('relative_volume_10d_calc') > 1,
                    Column('ADX') > 14,
                    )
        .get_scanner_data())[1]
    return Tarama

def RsiDonus():
    Tarama = (Query().set_markets('turkey')
                .select('name', 'change','close','volume','Perf.W')
                .where(
                    Column('RSI5').crosses_above(Column('RSI21')),
                    Column('volume') > Column('average_volume_10d_calc'),
                    Column('RSI') .crosses_above(30),
                    )
        .get_scanner_data())[1]
    return Tarama


# Verileri güncelleme fonksiyonu
def update_data():
    # Tarama fonksiyonlarını çağırın
    Sahmeran_df = Sahmeran()
    Vira_df = Vira()
    Longcu_df = Longcu()
    Burada41_df = Burada41()
    DayTrade_df = DayTrade()
    FridaKahlo_df = FridaKahlo()
    HodriMeydan_df = HodriMeydan()
    Pasa_df = Pasa()
    Ekmek_df = Ekmek()
    Flz_df = Flz()
    Macd_df = Macd()
    Alp_df = Alp()
    AdınıSenKoy_df = AdınıSenKoy()
    DuseniKiran_df = DuseniKiran()
    AtilganDuseniKiran_df = AtilganDuseniKiran()
    Tiga_df = Tiga()
    RsiZkn_df = RsiZkn()
    Oz_df = Oz()
    RsiDip_df = RsiDip()
    RsiDonus_df = RsiDonus()
    DuseniKiranYeni_df = DuseniKiranYeni()
    
    # Boş veya tümü NA olan DataFrame'leri hariç tut
    tarama_list = [df for df in [Sahmeran_df, Vira_df, Longcu_df, Burada41_df, DayTrade_df, FridaKahlo_df, HodriMeydan_df, Pasa_df, Ekmek_df, Flz_df, Macd_df, Alp_df, AdınıSenKoy_df, DuseniKiran_df, AtilganDuseniKiran_df, Tiga_df, RsiZkn_df, Oz_df, RsiDip_df, RsiDonus_df, DuseniKiranYeni_df] if not df.empty]

    # Taramaları birleştirin ve etiketleyin
    tarama_dict = {
        'Şahmeran': Sahmeran_df, 'Vira': Vira_df, 'Longcu': Longcu_df, '41Burada': Burada41_df, 'DayTrade': DayTrade_df,
        'FridaKahlo': FridaKahlo_df, 'HodriMeydan': HodriMeydan_df, 'Paşa': Pasa_df, 'Ekmek': Ekmek_df, 'Flz': Flz_df,
        'Macd': Macd_df, 'Alp': Alp_df, 'AdınıSenKoy': AdınıSenKoy_df, 'DüşeniKıran': DuseniKiran_df, 'Atılgan+DüşeniKıran': AtilganDuseniKiran_df,
        'Tiga': Tiga_df, 'RsiZkn': RsiZkn_df, 'Öz': Oz_df, 'RsiDip' : RsiDip_df, 'RsiDönüş' : RsiDonus_df, 'DuseniKiranYeni' : DuseniKiranYeni_df
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
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!A:Z'
    )
    clear_request.execute()

    # Verileri Google Sheets'e yazın
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!A1',
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
