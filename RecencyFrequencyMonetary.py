#############################################################
# RFM İle müşteri segmentasyonu (Customer segmentation with RFM)
#############################################################




# 1. İş problemi(Bussines problem)
#############################################################


# e-ticaret sistemini segmentlere ayırıp bu segmentlere göre
# pazar stratejisi belirlemek istiyor.


# Veri Seti Hikayesi
# https://archive.ics.uci.edu/ml/datasets/Online+Retail+II


# Online Retail II isimli veri seti İngiltere merkezli online bir satış mağazasının
# 01/12/2009 - 09/12/2011 tarihleri arasındaki satışlarını içeriyor.


# Değişkenler
#
# InvoiceNo: Fatura numarası. Her işleme yani faturaya ait eşsiz numara. C ile başlıyorsa iptal edilen işlem.
# StockCode: Ürün kodu. Her bir ürün için eşsiz numara.
# Description: Ürün ismi
# Quantity: Ürün adedi. Faturalardaki ürünlerden kaçar tane satıldığını ifade etmektedir.
# InvoiceDate: Fatura tarihi ve zamanı.
# UnitPrice: Ürün fiyatı (Sterlin cinsinden)
# CustomerID: Eşsiz müşteri numarası


# 2. Veriyi anlama (Data understanding)
#############################################################
import datetime as dt
import pandas as pd


pd.set_option('display.max_columns', None)  # Tüm sutunları görüntüleme
# pd.set_option('display.max_rows',None) # Tüm satırları görüntüleme
pd.set_option('display.float_format', lambda x: '%.5f' % x)  # sayısal değişkenlerin virgülden sonraki değer hassasiyeti


df_ = pd.read_excel("datasets/online_retail_II.xlsx", "Year 2009-2010")
df = df_.copy()


df.head(20)  # ilk 20 veri gösteriri
df.shape  # verinin boyutu
df.isnull().sum()  # kaç tane null veri var. Customer ID değerlerini kaldıracağız. nedeni ise müsteri bazlı segmente yapılacağı için.
df["Description"].nunique()  # essiz değeri kaç tane
df["Description"].value_counts().head()  # en çok satılan ilk 5 ürün


df.groupby("Description").agg({'Quantity': "sum"}).head()


df.groupby("Description").agg({'Quantity': "sum"}).sort_values("Quantity", ascending=False)


df["Invoice"].nunique()  # essiz değeri kaç tane


df['TotalPrice'] = df['Quantity'] * df['Price']


df.groupby("Invoice").agg({"TotalPrice": "sum"}).head()


## 3. Veriyi Hazırlama ( Data Preparation )
##########################################################
df.shape
df.isnull().sum()
df.dropna(inplace=True)  # Eksik değerleri sil. Değişikliği kalıcıdır.
df.describe().T


df = df[~df["Invoice"].str.contains("C", na=False)]  # Başında c olan faturaları getirme.


# 4.RFM Metriklerinin Hesaplanması ( Calculating RFM Metrics )
###############################################################
# Recency, Frequency, Monetary
df.head()
df["InvoiceDate"].max()


todat_date = dt.datetime(2010, 12, 11)
type(todat_date)


rfm = df.groupby('Customer ID').agg({'InvoiceDate': lambda InvoiceDate: (todat_date - InvoiceDate.max()).days,
                                    'Invoice': lambda Invoice: Invoice.nunique(),
                                    'TotalPrice': lambda TotalPrice: TotalPrice.sum()})
rfm.head()


rfm.columns = ['recency', 'frequency', 'monetary']
rfm.describe().T
rfm = rfm[rfm["monetary"] > 0]
rfm.describe().T
rfm.shape


# 5.RFM Skorlarının Hesaplanması ( Calculating RFM Scores)
##########################################################


rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
# qcut: quantily lara yani çeyrek değerlere göre bölme işlemi yapar.
# qcut(değişken, kaç parçaya bölünecek, labeller etiketi )  recency değeri küçükten büyüğe sıralanır.


#0-100, 0-20, 20-40, 40-60, 60-80, 80-100,


rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])




rfm['monetary_score'] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])


#Rfm skoru oluşturduk
rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))
rfm.describe().T


rfm[rfm["RFM_SCORE"] == "55"]
rfm[rfm["RFM_SCORE"] == "11"]


# 6.RFM Segmentlerinin Oluşturulması ( Creating & Analysing RFM Segments )
##########################################################################


#regex
# RFM isimlendirmesi
seg_map = {
   r'[1-2][1-2]': 'hibernating',
   r'[1-2][3-4]': 'at_Risk',
   r'[1-2]5': 'cant_loose',
   r'3[1-2]': 'about_to_sleep',
   r'33': 'need_attention',
   r'[3-4][4-5]': 'loyal_customers',
   r'41': 'promising',
   r'51': 'new_customers',
   r'[4-5][2-3]': 'potential_loyalists',
   r'5[4-5]': 'champions'
}




rfm["segment"] = rfm["RFM_SCORE"].replace(seg_map,regex=True)#Birleştirilen score


rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])


rfm[rfm["segment"]=="cant_loose"].head()


rfm[rfm["segment"]=="cant_loose"].index


new_df = pd.DataFrame()
new_df["new_customer_id"] = rfm[rfm["segment"] == "new_customers"].index
new_df["new_customer_id"] = new_df["new_customer_id"].astype(int)
new_df.to_csv("new_customers.csv")
rfm.to_csv("rfm.csv")


###############################################################
# 7. Tüm Sürecin Fonksiyonlaştırılması
###############################################################


def create_rfm(dataframe, csv=False):


   # VERIYI HAZIRLAMA
   dataframe["TotalPrice"] = dataframe["Quantity"] * dataframe["Price"]
   dataframe.dropna(inplace=True)
   dataframe = dataframe[~dataframe["Invoice"].str.contains("C", na=False)]


   # RFM METRIKLERININ HESAPLANMASI
   today_date = dt.datetime(2011, 12, 11)
   rfm = dataframe.groupby('Customer ID').agg({'InvoiceDate': lambda date: (today_date - date.max()).days,
                                               'Invoice': lambda num: num.nunique(),
                                               "TotalPrice": lambda price: price.sum()})
   rfm.columns = ['recency', 'frequency', "monetary"]
   rfm = rfm[(rfm['monetary'] > 0)]


   # RFM SKORLARININ HESAPLANMASI
   rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
   rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
   rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])


   # cltv_df skorları kategorik değere dönüştürülüp df'e eklendi
   rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) +
                       rfm['frequency_score'].astype(str))




   # SEGMENTLERIN ISIMLENDIRILMESI
   seg_map = {
       r'[1-2][1-2]': 'hibernating',
       r'[1-2][3-4]': 'at_risk',
       r'[1-2]5': 'cant_loose',
       r'3[1-2]': 'about_to_sleep',
       r'33': 'need_attention',
       r'[3-4][4-5]': 'loyal_customers',
       r'41': 'promising',
       r'51': 'new_customers',
       r'[4-5][2-3]': 'potential_loyalists',
       r'5[4-5]': 'champions'
   }


   rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)
   rfm = rfm[["recency", "frequency", "monetary", "segment"]]
   rfm.index = rfm.index.astype(int)


   if csv:
       rfm.to_csv("rfm.csv")


   return rfm


df = df_.copy()


rfm_new = create_rfm(df, csv=True)




