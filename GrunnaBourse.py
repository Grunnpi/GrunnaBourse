import requests
import json
import os
import argparse
import csv
import os.path
import urllib.parse
import sys

from bs4 import BeautifulSoup

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


GrunnaBourse = 'v0'

proxies = {}
sep = ","

def callParameter(customParameters):
    tabParameter = {
        'draw':'3',
        'columns[0][name]':'ID_Produit',
        'columns[1][name]':'cTypeFinancialItem',
        'columns[2][name]':'cClasseFinancialItem',
        'columns[3][name]':'sTypeFinancialObject',
        'columns[4][name]':'checkbox',
        'columns[5][name]':'sNom',
        'columns[6][name]':'sURL',
        'columns[7][name]':'sNomManager',
        'columns[8][name]':'sNomTypeFinancialObject',
        'columns[9][name]':'sGroupeCat_Specific_Dynamic',
        'columns[10][name]':'sGroupeCat_rng1',
        'columns[11][name]':'sCodeISIN',
        'columns[12][name]':'nVL',
        'columns[13][name]':'sCurrency',
        'columns[14][name]':'nStarRating',
        'columns[15][name]':'nScore',
        'columns[16][name]':'nRetYTD',
        'columns[17][name]':'nRet1a',
        'columns[18][name]':'nRet3a',
        'columns[19][name]':'nRet5a',
        'columns[20][name]':'nRet1c',
        'columns[21][name]':'nRet3c',
        'columns[22][name]':'nRet1j',
        'columns[23][name]':'nRet1m',
        'columns[24][name]':'nRet3m',
        'columns[25][name]':'nRet6m',
        'columns[26][name]':'nRet5c',
        'columns[27][name]':'nRet8c',
        'columns[28][name]':'nVolat1a',
        'columns[29][name]':'nVolat3a',
        'columns[30][name]':'nVolat5a',
        'columns[31][name]':'nSharpe1a',
        'columns[32][name]':'nSharpe3a',
        'columns[33][name]':'nSharpe5a',
        'columns[34][name]':'nPerteMax1a',
        'columns[35][name]':'nPerteMax3a',
        'columns[36][name]':'nPerteMax5a',
        'columns[37][name]':'nSortino1a',
        'columns[38][name]':'nSortino3a',
        'columns[39][name]':'nSortino5a',
        'columns[40][name]':'nIr1A',
        'columns[41][name]':'nIr3A',
        'columns[42][name]':'nIr5A',
        'columns[43][name]':'nMinInvest',
        'columns[44][name]':'nFraisGestion',
        'columns[45][name]':'nFraisEntree',
        'columns[46][name]':'nFraisSortie',
        'columns[47][name]':'dtRet',
        'columns[48][name]':'dtRetMonth',
        'columns[49][name]':'bFerme',
        'columns[50][name]':'nIntensiteESG',
        'columns[51][name]':'nActifEur',
        'columns[52][name]':'nActifDiffEUR1m',
        'columns[53][name]':'nActifDiffEUR3m',
        'columns[54][name]':'nActifDiffEUR6m',
        'columns[55][name]':'nActifDiffEUR1A',
        'columns[56][name]':'nActifCompartimentEUR',
        'columns[57][name]':'nActifCompartimentDiffEUR1m',
        'columns[58][name]':'nActifCompartimentDiffEUR3m',
        'columns[59][name]':'nActifCompartimentDiffEUR6m',
        'columns[60][name]':'nActifCompartimentDiffEUR1A',
        'columns[61][name]':'nCollecteCompartiment1m',
        'columns[62][name]':'nCollecteCompartiment3m',
        'columns[63][name]':'nCollecteCompartiment6m',
        'columns[64][name]':'nCollecteCompartimentYTD',
        'columns[65][name]':'nCollecteCompartiment1A',
        'columns[66][name]':'nCollecteCompartiment3A',
        'columns[67][name]':'nCollecteRet1m',
        'columns[68][name]':'nCollecteRet3m',
        'columns[69][name]':'nCollecteRet6m',
        'columns[70][name]':'nCollecteRetYTD',
        'columns[71][name]':'nCollecteRet1A',
        'columns[72][name]':'nCollecteRet3A',
        'columns[73][name]':'nCollecteCompartimentRet1m',
        'columns[74][name]':'nCollecteCompartimentRet3m',
        'columns[75][name]':'nCollecteCompartimentRet6m',
        'columns[76][name]':'nCollecteCompartimentRetYTD',
        'columns[77][name]':'nCollecteCompartimentRet1A',
        'columns[78][name]':'nCollecteCompartimentRet3A',
        'columns[79][name]':'isESG',
        'columns[80][name]':'sArticleSFDR',
        'columns[81][name]':'nIntensiteISR',
        'columns[82][name]':'nESG_Environnement',
        'columns[83][name]':'nESG_Social',
        'columns[84][name]':'nESG_Gouvernance',
        'columns[85][name]':'nNbLabels',
        'columns[86][name]':'sProspectusUrl',
        'columns[87][name]':'sUrlMainDocument',
        'columns[88][name]':'isMainDocumentAccessible',
        'order[0][column]':'5',
        'order[0][dir]':'asc',
        'start':'0',
        'length':'300',
        'search[value]':'',
        'search[regex]':'false',
        'nbMaxCompare':'5',
        'Values.sNomOrISIN':'',
        'Values.bETF':'true',
        'Values.isTypeProduitV2':'true',
        'Values.sDevise':'EUR',
        'Values.nAge':'',
        'Values.sDomicile':'',
        'Values.nTypeFonds':'',
        'Values.bPEA':'true',
        'Values.nTypeInvestisseur':'',
        'Values.nDistribution':'',
        'Values.nAMF':'',
        'Values.bExcludeUncommercialized':'true',
        'Values.perfAnnu.dateIndex':'0',
        'Values.perfAnnu.signe':'ge',
        'Values.perfAnnu.value':'',
        'Values.perfCumulee.sDate':'0',
        'Values.perfCumulee.signe':'ge',
        'Values.perfCumulee.value':'',
        'Values.superfAnnu.dateIndex':'0',
        'Values.superfAnnu.signe':'le',
        'Values.superfAnnu.value':'',
        'Values.sharpe.dateIndex':'0',
        'Values.sharpe.signe':'ge',
        'Values.sharpe.value':'',
        'Values.volat.dateIndex':'0',
        'Values.volat.signe':'le',
        'Values.volat.value':'',
        'Values.perteMax.dateIndex':'0',
        'Values.perteMax.signe':'le',
        'Values.perteMax.value':'',
        'Values.beta.dateIndex':'0',
        'Values.beta.signe':'le',
        'Values.beta.value':'',
        'Values.ecartSuivi.dateIndex':'0',
        'Values.ecartSuivi.signe':'le',
        'Values.ecartSuivi.value':'',
        'Values.IR.dateIndex':'0',
        'Values.IR.signe':'le',
        'Values.IR.value':'',
        'Values.sortino.dateIndex':'0',
        'Values.sortino.signe':'le',
        'Values.sortino.value':'',
        'Values.ratioOmega.dateIndex':'0',
        'Values.ratioOmega.signe':'le',
        'Values.ratioOmega.value':'',
        'Values.betaHaussier.dateIndex':'0',
        'Values.betaHaussier.signe':'le',
        'Values.betaHaussier.value':'',
        'Values.betaBaissier.dateIndex':'0',
        'Values.betaBaissier.signe':'le',
        'Values.betaBaissier.value':'',
        'Values.upCaptureRatio.dateIndex':'0',
        'Values.upCaptureRatio.signe':'le',
        'Values.upCaptureRatio.value':'',
        'Values.downCaptureRatio.dateIndex':'0',
        'Values.downCaptureRatio.signe':'le',
        'Values.downCaptureRatio.value':'',
        'Values.DSR.dateIndex':'0',
        'Values.DSR.signe':'le',
        'Values.DSR.value':'',
        'Values.var95.dateIndex':'0',
        'Values.var95.signe':'le',
        'Values.var95.value':'',
        'Values.var99.dateIndex':'0',
        'Values.var99.signe':'le',
        'Values.var99.value':'',
        'Values.skewness.dateIndex':'0',
        'Values.skewness.signe':'le',
        'Values.skewness.value':'',
        'Values.kurtosis.dateIndex':'0',
        'Values.kurtosis.signe':'le',
        'Values.kurtosis.value':'',
        'Values.fraisSouscription.Signe':'le',
        'Values.fraisSouscription.Value':'',
        'Values.fraisRachat.Signe':'le',
        'Values.fraisRachat.Value':'',
        'Values.fraisGestion.Signe':'le',
        'Values.fraisGestion.Value':'',
        'Values.fraisCourants.Signe':'le',
        'Values.fraisCourants.Value':'',
        'Values.ESG.isEnvironnement':'',
        'Values.ESG.isSocial':'',
        'Values.ESG.isGouvernance':'',
        'Values.isIntersectionContrats':'false',
        'sNomOrISIN':'',
        'Values.isForProposition':'false',
    }
    for key, value in customParameters.items():
        tabParameter[key] = value
    return tabParameter


class UnETF:
    "ETF"
    sNom = ''
    nActifEur = ''
    sCodeISIN = ''
    nVL = ''
    nFraisGestion = ''
    nFraisEntree = ''
    nFraisSortie = ''
    sGroupeCat_Specific_Dynamic = ''

    # perf
    nRet1a = ''
    nRet3a = ''
    nRet5a = ''
    # volatility
    nVolat1a = ''
    nVolat3a = ''
    nVolat5a = ''

    # DETAILS
    typeInvestisseur = ''
    classificationAMF = ''
    indiceDeReference = ''
    categorieQ = ''
    indiceDeRefQ = ''

    capiDitri = ''
    partHedgee = ''
    eligiblePEA = ''


    def __init__(self, etf):
        self.sNom = str(etf['sNom'])
        self.nActifEur = str(etf['nActifEur'])
        self.sCodeISIN = str(etf['sCodeISIN'])
        self.nVL = str(etf['nVL'])

        self.nFraisGestion= str(etf['nFraisGestion'])
        self.nFraisEntree = str(etf['nFraisEntree'])
        self.nFraisSortie = str(etf['nFraisSortie'])

        self.sGroupeCat_Specific_Dynamic = str(etf['sGroupeCat_Specific_Dynamic'])

        self.nRet1a = str(etf['nRet1a'])
        self.nRet3a = str(etf['nRet3a'])
        self.nRet5a = str(etf['nRet5a'])

        self.nVolat1a = str(etf['nVolat1a'])
        self.nVolat3a = str(etf['nVolat3a'])
        self.nVolat5a = str(etf['nVolat5a'])


    def toString(self, sep):
        """Format du dump fichier"""
        return "" \
                    + self.sCodeISIN \
               + sep + self.nActifEur \
               + sep + self.sNom \
               + sep + self.nVL \
               + sep + self.nFraisGestion \
               + sep + self.nFraisEntree \
               + sep + self.nFraisSortie \
               + sep + self.sGroupeCat_Specific_Dynamic \
               + sep + self.typeInvestisseur \
               + sep + self.classificationAMF \
               + sep + self.indiceDeReference \
               + sep + self.categorieQ \
               + sep + self.indiceDeRefQ \
               + sep + self.nRet1a \
               + sep + self.nRet3a \
               + sep + self.nRet5a \
               + sep + self.nVolat1a \
               + sep + self.nVolat3a \
               + sep + self.nVolat5a \
               + ""

def cleanValue(value):
    value = str(value)
    return value.replace("\n", "").strip()

def getDetail(sURL, unETF,proxies):

    r = requests.get("https://www.quantalys.com/" + sURL, proxies=proxies, verify=False)
    if r.status_code != 200:
        print(r.status_code, r.reason)

    soup = BeautifulSoup(r.text, 'html.parser')
    firstHeader = soup.find('dl', class_='dl-fichier-identite')
    detailsHeader = firstHeader.find_all('dd')
    # index = 0
    # for detailHeader in detailsHeader:
    #     print("[" + str(index) + "] = <" + detailHeader.text + ">")
    #     index = index + 1
    unETF.typeInvestisseur = cleanValue(detailsHeader[1].text)
    unETF.classificationAMF = cleanValue(detailsHeader[2].text)
    unETF.indiceDeReference = cleanValue(detailsHeader[3].text)
    unETF.categorieQ = cleanValue(detailsHeader[4].text)
    unETF.indiceDeRefQ = cleanValue(detailsHeader[5].text)

    dataRef = soup.find('div', { "data-ref" : "root-synthese-fond" })
    dataRefs = dataRef.find_all('div', recursive=False)
    subDataRefs = dataRefs[4].find_all('div',class_="col-md-4")
    caracGenerales = subDataRefs[2].find_all('td')
    index = 0
    for caracGenerale in caracGenerales:
        print("caracGenerale[" + str(index) + "] = " + caracGenerale.text)
        index = index + 1

    # exit(-1)


# partie principale
if __name__ == "__main__":

    customParameters = { 'length' : '3' }
    callParameters = callParameter(customParameters)

    callString = ""
    index = 0
    for key, value in callParameters.items():
        index = index + 1
        if index != 1:
            callString = callString + "&"
        callString = callString + urllib.parse.quote_plus(key) + "=" + urllib.parse.quote_plus(value)

    parser=argparse.ArgumentParser(description='GrunnaBourse extact process')

    # Ecole Directe cred
    parser.add_argument('--proxy', help='Proxy if behind firewall : https://uzer:pwd@name:port', type=str, default="")
    parser.print_help()

    args=parser.parse_args()


    if args.proxy:
        print("Proxy provided")
        proxies = {
            "https": str(args.proxy)
        }

    # payload = "draw=4&columns%5B0%5D%5Bname%5D=ID_Produit&columns%5B1%5D%5Bname%5D=cTypeFinancialItem&columns%5B2%5D%5Bname%5D=cClasseFinancialItem&columns%5B3%5D%5Bname%5D=sTypeFinancialObject&columns%5B4%5D%5Bname%5D=checkbox&columns%5B5%5D%5Bname%5D=sNom&columns%5B6%5D%5Bname%5D=sURL&columns%5B7%5D%5Bname%5D=sNomManager&columns%5B8%5D%5Bname%5D=sNomTypeFinancialObject&columns%5B9%5D%5Bname%5D=sGroupeCat_Specific_Dynamic&columns%5B10%5D%5Bname%5D=sGroupeCat_rng1&columns%5B11%5D%5Bname%5D=sCodeISIN&columns%5B12%5D%5Bname%5D=nVL&columns%5B13%5D%5Bname%5D=sCurrency&columns%5B14%5D%5Bname%5D=nStarRating&columns%5B15%5D%5Bname%5D=nScore&columns%5B16%5D%5Bname%5D=nRetYTD&columns%5B17%5D%5Bname%5D=nRet1a&columns%5B18%5D%5Bname%5D=nRet3a&columns%5B19%5D%5Bname%5D=nRet5a&columns%5B20%5D%5Bname%5D=nRet1c&columns%5B21%5D%5Bname%5D=nRet3c&columns%5B22%5D%5Bname%5D=nRet1j&columns%5B23%5D%5Bname%5D=nRet1m&columns%5B24%5D%5Bname%5D=nRet3m&columns%5B25%5D%5Bname%5D=nRet6m&columns%5B26%5D%5Bname%5D=nRet5c&columns%5B27%5D%5Bname%5D=nRet8c&columns%5B28%5D%5Bname%5D=nVolat1a&columns%5B29%5D%5Bname%5D=nVolat3a&columns%5B30%5D%5Bname%5D=nVolat5a&columns%5B31%5D%5Bname%5D=nSharpe1a&columns%5B32%5D%5Bname%5D=nSharpe3a&columns%5B33%5D%5Bname%5D=nSharpe5a&columns%5B34%5D%5Bname%5D=nPerteMax1a&columns%5B35%5D%5Bname%5D=nPerteMax3a&columns%5B36%5D%5Bname%5D=nPerteMax5a&columns%5B37%5D%5Bname%5D=nSortino1a&columns%5B38%5D%5Bname%5D=nSortino3a&columns%5B39%5D%5Bname%5D=nSortino5a&columns%5B40%5D%5Bname%5D=nIr1A&columns%5B41%5D%5Bname%5D=nIr3A&columns%5B42%5D%5Bname%5D=nIr5A&columns%5B43%5D%5Bname%5D=nMinInvest&columns%5B44%5D%5Bname%5D=nFraisGestion&columns%5B45%5D%5Bname%5D=nFraisEntree&columns%5B46%5D%5Bname%5D=nFraisSortie&columns%5B47%5D%5Bname%5D=dtRet&columns%5B48%5D%5Bname%5D=dtRetMonth&columns%5B49%5D%5Bname%5D=bFerme&columns%5B50%5D%5Bname%5D=nIntensiteESG&columns%5B51%5D%5Bname%5D=nActifEur&columns%5B52%5D%5Bname%5D=nActifDiffEUR1m&columns%5B53%5D%5Bname%5D=nActifDiffEUR3m&columns%5B54%5D%5Bname%5D=nActifDiffEUR6m&columns%5B55%5D%5Bname%5D=nActifDiffEUR1A&columns%5B56%5D%5Bname%5D=nActifCompartimentEUR&columns%5B57%5D%5Bname%5D=nActifCompartimentDiffEUR1m&columns%5B58%5D%5Bname%5D=nActifCompartimentDiffEUR3m&columns%5B59%5D%5Bname%5D=nActifCompartimentDiffEUR6m&columns%5B60%5D%5Bname%5D=nActifCompartimentDiffEUR1A&columns%5B61%5D%5Bname%5D=nCollecteCompartiment1m&columns%5B62%5D%5Bname%5D=nCollecteCompartiment3m&columns%5B63%5D%5Bname%5D=nCollecteCompartiment6m&columns%5B64%5D%5Bname%5D=nCollecteCompartimentYTD&columns%5B65%5D%5Bname%5D=nCollecteCompartiment1A&columns%5B66%5D%5Bname%5D=nCollecteCompartiment3A&columns%5B67%5D%5Bname%5D=nCollecteRet1m&columns%5B68%5D%5Bname%5D=nCollecteRet3m&columns%5B69%5D%5Bname%5D=nCollecteRet6m&columns%5B70%5D%5Bname%5D=nCollecteRetYTD&columns%5B71%5D%5Bname%5D=nCollecteRet1A&columns%5B72%5D%5Bname%5D=nCollecteRet3A&columns%5B73%5D%5Bname%5D=nCollecteCompartimentRet1m&columns%5B74%5D%5Bname%5D=nCollecteCompartimentRet3m&columns%5B75%5D%5Bname%5D=nCollecteCompartimentRet6m&columns%5B76%5D%5Bname%5D=nCollecteCompartimentRetYTD&columns%5B77%5D%5Bname%5D=nCollecteCompartimentRet1A&columns%5B78%5D%5Bname%5D=nCollecteCompartimentRet3A&columns%5B79%5D%5Bname%5D=isESG&columns%5B80%5D%5Bname%5D=sArticleSFDR&columns%5B81%5D%5Bname%5D=nIntensiteISR&columns%5B82%5D%5Bname%5D=nESG_Environnement&columns%5B83%5D%5Bname%5D=nESG_Social&columns%5B84%5D%5Bname%5D=nESG_Gouvernance&columns%5B85%5D%5Bname%5D=nNbLabels&columns%5B86%5D%5Bname%5D=sProspectusUrl&columns%5B87%5D%5Bname%5D=sUrlMainDocument&columns%5B88%5D%5Bname%5D=isMainDocumentAccessible&order%5B0%5D%5Bcolumn%5D=5&order%5B0%5D%5Bdir%5D=asc&start=0&length=100&search%5Bvalue%5D=&search%5Bregex%5D=false&nbMaxCompare=5&Values.sNomOrISIN=&Values.bETF=true&Values.isTypeProduitV2=true&Values.sDevise=EUR&Values.nAge=&Values.sDomicile=&Values.nTypeFonds=&Values.bPEA=true&Values.nTypeInvestisseur=1&Values.nDistribution=4&Values.nAMF=&Values.bExcludeUncommercialized=true&Values.perfAnnu.dateIndex=0&Values.perfAnnu.signe=ge&Values.perfAnnu.value=&Values.perfCumulee.sDate=0&Values.perfCumulee.signe=ge&Values.perfCumulee.value=&Values.superfAnnu.dateIndex=0&Values.superfAnnu.signe=le&Values.superfAnnu.value=&Values.sharpe.dateIndex=0&Values.sharpe.signe=ge&Values.sharpe.value=&Values.volat.dateIndex=0&Values.volat.signe=le&Values.volat.value=&Values.perteMax.dateIndex=0&Values.perteMax.signe=le&Values.perteMax.value=&Values.beta.dateIndex=0&Values.beta.signe=le&Values.beta.value=&Values.ecartSuivi.dateIndex=0&Values.ecartSuivi.signe=le&Values.ecartSuivi.value=&Values.IR.dateIndex=0&Values.IR.signe=le&Values.IR.value=&Values.sortino.dateIndex=0&Values.sortino.signe=le&Values.sortino.value=&Values.ratioOmega.dateIndex=0&Values.ratioOmega.signe=le&Values.ratioOmega.value=&Values.betaHaussier.dateIndex=0&Values.betaHaussier.signe=le&Values.betaHaussier.value=&Values.betaBaissier.dateIndex=0&Values.betaBaissier.signe=le&Values.betaBaissier.value=&Values.upCaptureRatio.dateIndex=0&Values.upCaptureRatio.signe=le&Values.upCaptureRatio.value=&Values.downCaptureRatio.dateIndex=0&Values.downCaptureRatio.signe=le&Values.downCaptureRatio.value=&Values.DSR.dateIndex=0&Values.DSR.signe=le&Values.DSR.value=&Values.var95.dateIndex=0&Values.var95.signe=le&Values.var95.value=&Values.var99.dateIndex=0&Values.var99.signe=le&Values.var99.value=&Values.skewness.dateIndex=0&Values.skewness.signe=le&Values.skewness.value=&Values.kurtosis.dateIndex=0&Values.kurtosis.signe=le&Values.kurtosis.value=&Values.fraisSouscription.Signe=le&Values.fraisSouscription.Value=&Values.fraisRachat.Signe=le&Values.fraisRachat.Value=&Values.fraisGestion.Signe=le&Values.fraisGestion.Value=&Values.fraisCourants.Signe=le&Values.fraisCourants.Value=&Values.ESG.isEnvironnement=&Values.ESG.isSocial=&Values.ESG.isGouvernance=&Values.isIntersectionContrats=false&sNomOrISIN=&Values.isForProposition=false"
    payload = callString
    headers = {'content-type': 'application/x-www-form-urlencoded'}

    # payload = urllib.parse.quote_plus(payload)
    #print(payload)

    r = requests.post("https://www.quantalys.com/Recherche/Data", data=payload, headers=headers, proxies=proxies, verify=False)
    if r.status_code != 200:
        print(r.status_code, r.reason)
    retourEnJson = json.loads(r.content)
    print("Generation des fichiers ici : [" + os.getcwd() + "]")

    liste_des_etf = []
    countETF = 0
    for etf in retourEnJson['data']:
        unETF = UnETF(etf)
        countETF = countETF + 1

        # get detailed
        getDetail(etf['sURL'], unETF, proxies)

        print(unETF.toString(sep))

    print("Fin extraction : " + str(countETF) + " ETF")

