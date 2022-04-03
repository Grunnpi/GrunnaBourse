import requests
import json
import os
import argparse
import csv
import os.path
import urllib.parse
import sys

from requests.packages.urllib3.exceptions import InsecureRequestWarning

import telegram

import gspread
from oauth2client.service_account import ServiceAccountCredentials

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


GrunnaBourse = 'v0'

proxies = {}
sep = ","

class UnETF:
    "ETF"
    sNom = ''
    nActifEur = ''
    sCodeISIN = ''
    nVL = ''
    nFraisGestion = ''
    nFraisEntree = ''
    nFraisSortie = ''

    def __init__(self, etf):
        self.sNom = str(etf['sNom'])
        self.nActifEur = str(etf['nActifEur'])
        self.sCodeISIN = str(etf['sCodeISIN'])
        self.nVL = str(etf['nVL'])

        self.nFraisGestion= str(etf['nFraisGestion'])
        self.nFraisEntree = str(etf['nFraisEntree'])
        self.nFraisSortie = str(etf['nFraisSortie'])


    def toString(self, sep):
        """Format du dump fichier"""
        return "" \
            + sep + self.sCodeISIN \
            + sep + self.nActifEur \
            + sep + self.sNom \
            + sep + self.nVL \
            + sep + self.nFraisGestion \
            + sep + self.nFraisEntree \
            + sep + self.nFraisSortie

def dump( champ, bulletProof ):
    returnMe = ""
    if ( bulletProof ):
        returnMe = repr(str(champ.encode('utf8')))[2:-1]
    else:
        returnMe = "'" + str(champ) + "'"
    returnMe = returnMe.replace("'", "\"")
    return returnMe

def listeNoteGoogle(sheetOnglet):
    all_kid_notes = []
    all_kid_notes_sheet = sheetOnglet.get_all_records()
    #
    for rec in all_kid_notes_sheet:
        uneNote = UneNote('', '', '', '', '', '', '', '','')
        for item in rec.items():
            # print(item[0], " -- ", item[1], "<", item, ">",)
            if ( item[0] == 'periode'):
                uneNote.periode = item[1]
            if ( item[0] == 'libelleMatiere'):
                uneNote.libelleMatiere = item[1]
            if ( item[0] == 'valeur'):
                uneNote.valeur = item[1]
            if ( item[0] == 'noteSur'):
                uneNote.noteSur = item[1]
            if ( item[0] == 'coef'):
                uneNote.coef = item[1]
            if ( item[0] == 'typeDevoir'):
                uneNote.typeDevoir = item[1]
            if ( item[0] == 'devoir'):
                uneNote.devoir = item[1]
            if ( item[0] == 'date'):
                uneNote.date = item[1]
        all_kid_notes.append(uneNote)

    all_kid_notes = sorted(all_kid_notes)
    return all_kid_notes

# fonction pour lister toutes les notes d'un eleve sur base de son ID
def listeNoteSite(eleve_id, token):
    all_kid_notes = []

    payloadNotes = "data={\"token\": \"" + token + "\"}"
    headersNotes = {'content-type': 'application/x-www-form-urlencoded'}
    r = requests.post("https://api.ecoledirecte.com/v3/eleves/" + str(eleve_id) + "/notes.awp?verbe=get&",
                      data=payloadNotes, headers=headersNotes, proxies=proxies, verify=False)
    if r.status_code != 200:
        print(r.status_code, r.reason)
    notesEnJSON = json.loads(r.content)
    if len(notesEnJSON['data']['notes']) > 0:
        for note in notesEnJSON['data']['notes']:
            uneNote = UneNote( \
                    note['codePeriode'] \
                ,   note['libelleMatiere'] \
                ,   note['valeur'].replace(".", ",") \
                ,   note['noteSur'] \
                ,   note['coef'].replace(".", ",") \
                ,   note['typeDevoir'] \
                ,   note['devoir'] \
                ,   note['date'] \
                ,   note['nonSignificatif'] \
                )
            all_kid_notes.append(uneNote)
        all_kid_notes = sorted(all_kid_notes)
    else:
        print("pas de notes encore")
    return all_kid_notes


# partie principale
if __name__ == "__main__":

    parser=argparse.ArgumentParser(description='GrunnaBourse extact process')

    # Ecole Directe cred
    parser.add_argument('--user', help='ED User', type=str, required=True)
    parser.add_argument('--pwd', help='ED Password', type=str, required=True)
    parser.add_argument('--proxy', help='Proxy if behind firewall : https://uzer:pwd@name:port', type=str, default="")
    # credential google
    parser.add_argument('--cred', help='Google Drive json credential file', type=str, required=True)
    # telegram mode
    parser.add_argument('--token', help='Telegram bot token', type=str, default="")
    parser.add_argument('--chatid', help='Telegram chatid', type=str, default="")
    parser.add_argument('--telegram', help='Telegram flag (use or not)', type=str, default="no")

    parser.print_help()

    args=parser.parse_args()

    # # use creds to create a client to interact with the Google Drive API
    # scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    # creds = ServiceAccountCredentials.from_json_keyfile_name(str(args.cred), scope)
    # client = gspread.authorize(creds)
    #
    # # collect all kids and related setup
    # notes_ConfigurationSheet = client.open("Notes_EcoleDirecte").worksheet("Configuration")
    # list_configuration = notes_ConfigurationSheet.get_all_records()
    # listeEnfants = []
    # for rec in list_configuration:
    #     unEnfant = UnEnfant()
    #     for item in rec.items():
    #         if ( item[0] == 'Prénom'):
    #             unEnfant.prenom = item[1]
    #             print(item[1])
    #         if ( item[0] == 'Onglet'):
    #             unEnfant.onglet = item[1]
    #             print(item[1])
    #     listeEnfants.append(unEnfant)
    #
    # print("**** sboub")

    if args.proxy:
        print("Proxy provided")
        proxies = {
            "https": str(args.proxy)
        }

    payload = "draw=4&columns%5B0%5D%5Bname%5D=ID_Produit&columns%5B1%5D%5Bname%5D=cTypeFinancialItem&columns%5B2%5D%5Bname%5D=cClasseFinancialItem&columns%5B3%5D%5Bname%5D=sTypeFinancialObject&columns%5B4%5D%5Bname%5D=checkbox&columns%5B5%5D%5Bname%5D=sNom&columns%5B6%5D%5Bname%5D=sURL&columns%5B7%5D%5Bname%5D=sNomManager&columns%5B8%5D%5Bname%5D=sNomTypeFinancialObject&columns%5B9%5D%5Bname%5D=sGroupeCat_Specific_Dynamic&columns%5B10%5D%5Bname%5D=sGroupeCat_rng1&columns%5B11%5D%5Bname%5D=sCodeISIN&columns%5B12%5D%5Bname%5D=nVL&columns%5B13%5D%5Bname%5D=sCurrency&columns%5B14%5D%5Bname%5D=nStarRating&columns%5B15%5D%5Bname%5D=nScore&columns%5B16%5D%5Bname%5D=nRetYTD&columns%5B17%5D%5Bname%5D=nRet1a&columns%5B18%5D%5Bname%5D=nRet3a&columns%5B19%5D%5Bname%5D=nRet5a&columns%5B20%5D%5Bname%5D=nRet1c&columns%5B21%5D%5Bname%5D=nRet3c&columns%5B22%5D%5Bname%5D=nRet1j&columns%5B23%5D%5Bname%5D=nRet1m&columns%5B24%5D%5Bname%5D=nRet3m&columns%5B25%5D%5Bname%5D=nRet6m&columns%5B26%5D%5Bname%5D=nRet5c&columns%5B27%5D%5Bname%5D=nRet8c&columns%5B28%5D%5Bname%5D=nVolat1a&columns%5B29%5D%5Bname%5D=nVolat3a&columns%5B30%5D%5Bname%5D=nVolat5a&columns%5B31%5D%5Bname%5D=nSharpe1a&columns%5B32%5D%5Bname%5D=nSharpe3a&columns%5B33%5D%5Bname%5D=nSharpe5a&columns%5B34%5D%5Bname%5D=nPerteMax1a&columns%5B35%5D%5Bname%5D=nPerteMax3a&columns%5B36%5D%5Bname%5D=nPerteMax5a&columns%5B37%5D%5Bname%5D=nSortino1a&columns%5B38%5D%5Bname%5D=nSortino3a&columns%5B39%5D%5Bname%5D=nSortino5a&columns%5B40%5D%5Bname%5D=nIr1A&columns%5B41%5D%5Bname%5D=nIr3A&columns%5B42%5D%5Bname%5D=nIr5A&columns%5B43%5D%5Bname%5D=nMinInvest&columns%5B44%5D%5Bname%5D=nFraisGestion&columns%5B45%5D%5Bname%5D=nFraisEntree&columns%5B46%5D%5Bname%5D=nFraisSortie&columns%5B47%5D%5Bname%5D=dtRet&columns%5B48%5D%5Bname%5D=dtRetMonth&columns%5B49%5D%5Bname%5D=bFerme&columns%5B50%5D%5Bname%5D=nIntensiteESG&columns%5B51%5D%5Bname%5D=nActifEur&columns%5B52%5D%5Bname%5D=nActifDiffEUR1m&columns%5B53%5D%5Bname%5D=nActifDiffEUR3m&columns%5B54%5D%5Bname%5D=nActifDiffEUR6m&columns%5B55%5D%5Bname%5D=nActifDiffEUR1A&columns%5B56%5D%5Bname%5D=nActifCompartimentEUR&columns%5B57%5D%5Bname%5D=nActifCompartimentDiffEUR1m&columns%5B58%5D%5Bname%5D=nActifCompartimentDiffEUR3m&columns%5B59%5D%5Bname%5D=nActifCompartimentDiffEUR6m&columns%5B60%5D%5Bname%5D=nActifCompartimentDiffEUR1A&columns%5B61%5D%5Bname%5D=nCollecteCompartiment1m&columns%5B62%5D%5Bname%5D=nCollecteCompartiment3m&columns%5B63%5D%5Bname%5D=nCollecteCompartiment6m&columns%5B64%5D%5Bname%5D=nCollecteCompartimentYTD&columns%5B65%5D%5Bname%5D=nCollecteCompartiment1A&columns%5B66%5D%5Bname%5D=nCollecteCompartiment3A&columns%5B67%5D%5Bname%5D=nCollecteRet1m&columns%5B68%5D%5Bname%5D=nCollecteRet3m&columns%5B69%5D%5Bname%5D=nCollecteRet6m&columns%5B70%5D%5Bname%5D=nCollecteRetYTD&columns%5B71%5D%5Bname%5D=nCollecteRet1A&columns%5B72%5D%5Bname%5D=nCollecteRet3A&columns%5B73%5D%5Bname%5D=nCollecteCompartimentRet1m&columns%5B74%5D%5Bname%5D=nCollecteCompartimentRet3m&columns%5B75%5D%5Bname%5D=nCollecteCompartimentRet6m&columns%5B76%5D%5Bname%5D=nCollecteCompartimentRetYTD&columns%5B77%5D%5Bname%5D=nCollecteCompartimentRet1A&columns%5B78%5D%5Bname%5D=nCollecteCompartimentRet3A&columns%5B79%5D%5Bname%5D=isESG&columns%5B80%5D%5Bname%5D=sArticleSFDR&columns%5B81%5D%5Bname%5D=nIntensiteISR&columns%5B82%5D%5Bname%5D=nESG_Environnement&columns%5B83%5D%5Bname%5D=nESG_Social&columns%5B84%5D%5Bname%5D=nESG_Gouvernance&columns%5B85%5D%5Bname%5D=nNbLabels&columns%5B86%5D%5Bname%5D=sProspectusUrl&columns%5B87%5D%5Bname%5D=sUrlMainDocument&columns%5B88%5D%5Bname%5D=isMainDocumentAccessible&order%5B0%5D%5Bcolumn%5D=5&order%5B0%5D%5Bdir%5D=asc&start=0&length=100&search%5Bvalue%5D=&search%5Bregex%5D=false&nbMaxCompare=5&Values.sNomOrISIN=&Values.bETF=true&Values.isTypeProduitV2=true&Values.sDevise=EUR&Values.nAge=&Values.sDomicile=&Values.nTypeFonds=&Values.bPEA=true&Values.nTypeInvestisseur=1&Values.nDistribution=4&Values.nAMF=&Values.bExcludeUncommercialized=true&Values.perfAnnu.dateIndex=0&Values.perfAnnu.signe=ge&Values.perfAnnu.value=&Values.perfCumulee.sDate=0&Values.perfCumulee.signe=ge&Values.perfCumulee.value=&Values.superfAnnu.dateIndex=0&Values.superfAnnu.signe=le&Values.superfAnnu.value=&Values.sharpe.dateIndex=0&Values.sharpe.signe=ge&Values.sharpe.value=&Values.volat.dateIndex=0&Values.volat.signe=le&Values.volat.value=&Values.perteMax.dateIndex=0&Values.perteMax.signe=le&Values.perteMax.value=&Values.beta.dateIndex=0&Values.beta.signe=le&Values.beta.value=&Values.ecartSuivi.dateIndex=0&Values.ecartSuivi.signe=le&Values.ecartSuivi.value=&Values.IR.dateIndex=0&Values.IR.signe=le&Values.IR.value=&Values.sortino.dateIndex=0&Values.sortino.signe=le&Values.sortino.value=&Values.ratioOmega.dateIndex=0&Values.ratioOmega.signe=le&Values.ratioOmega.value=&Values.betaHaussier.dateIndex=0&Values.betaHaussier.signe=le&Values.betaHaussier.value=&Values.betaBaissier.dateIndex=0&Values.betaBaissier.signe=le&Values.betaBaissier.value=&Values.upCaptureRatio.dateIndex=0&Values.upCaptureRatio.signe=le&Values.upCaptureRatio.value=&Values.downCaptureRatio.dateIndex=0&Values.downCaptureRatio.signe=le&Values.downCaptureRatio.value=&Values.DSR.dateIndex=0&Values.DSR.signe=le&Values.DSR.value=&Values.var95.dateIndex=0&Values.var95.signe=le&Values.var95.value=&Values.var99.dateIndex=0&Values.var99.signe=le&Values.var99.value=&Values.skewness.dateIndex=0&Values.skewness.signe=le&Values.skewness.value=&Values.kurtosis.dateIndex=0&Values.kurtosis.signe=le&Values.kurtosis.value=&Values.fraisSouscription.Signe=le&Values.fraisSouscription.Value=&Values.fraisRachat.Signe=le&Values.fraisRachat.Value=&Values.fraisGestion.Signe=le&Values.fraisGestion.Value=&Values.fraisCourants.Signe=le&Values.fraisCourants.Value=&Values.ESG.isEnvironnement=&Values.ESG.isSocial=&Values.ESG.isGouvernance=&Values.isIntersectionContrats=false&sNomOrISIN=&Values.isForProposition=false"
    # payload = urllib.parse.quote_plus(payload)


    headers = {'content-type': 'application/x-www-form-urlencoded'}
    #print(payload)

    r = requests.post("https://www.quantalys.com/Recherche/Data", data=payload, headers=headers, proxies=proxies, verify=False)
    if r.status_code != 200:
        print(r.status_code, r.reason)
    retourEnJson = json.loads(r.content)
    print("Generation des fichiers ici : [" + os.getcwd() + "]")

    liste_des_etf = []
    compteurTotalNouvelleNote = 0
    for etf in retourEnJson['data']:
        unETF = UnETF(etf)
        print(unETF.toString(sep))

    exit(-1)

    telegram_message = "*GrunnaBourse(" + EcoleDirectVersion + ")* "
    compteurTotalNouvelleNote = 0
    for etf in retourEnJson['data']:
        sCodeISIN = str(etf['sCodeISIN'])
        eleveId = str(eleve['id'])
        elevePrenom = str(eleve['prenom'])
        print("Eleve(" + eleveId + ")[" + elevePrenom + "]")
        trouveEleve = False
        nbCreate = 0
        erreurApiMax = False
        for x in listeEnfants:
            if x.prenom == elevePrenom:
                print(">> Eleve config trouvé : ", x.onglet)
                trouveEleve = True
                print(">>> Extract Google")

                sheet_ongleNotes = client.open("Notes_EcoleDirecte").worksheet(x.onglet)
                eleveNotesDansGoogle = listeNoteGoogle(sheet_ongleNotes)
                print(">>> Extract Site")
                eleveNotesDansSite = listeNoteSite(eleveId, retourEnJson['token'])

                print("Notes dans google[", len(eleveNotesDansGoogle), "] / notes sur site [", len(eleveNotesDansSite), "]")
                googleNextRow = len(eleveNotesDansGoogle) + 2 # header + new row

                inventaireNote = ""
                for uneNoteSite in eleveNotesDansSite:
                    isNoteSiteDejaSurGoogle = False
                    for uneNoteGoogle in eleveNotesDansGoogle:
                        if ( uneNoteSite == uneNoteGoogle ):
                            isNoteSiteDejaSurGoogle = True
                            break
                    if ( not isNoteSiteDejaSurGoogle ):
                        print("Ajoute %s" % uneNoteSite.valeur, " @ ligne %d" % googleNextRow)
                        theValeur = uneNoteSite.valeur
                        if ( uneNoteSite.valeur.replace(",", ".").isnumeric()):
                            theValeur = float(theValeur.replace(",", "."))
                        theNoteSur = uneNoteSite.noteSur
                        if ( uneNoteSite.noteSur.replace(",", ".").isnumeric()):
                            theNoteSur = float(theNoteSur.replace(",", "."))
                        theCoef = uneNoteSite.coef
                        if ( uneNoteSite.coef.replace(",", ".").isnumeric()):
                            theCoef = float(theCoef.replace(",", "."))

                        VRAI_NOTE = 'TRUE'
                        if ( uneNoteSite.nonSignificatif == True ):
                            VRAI_NOTE = 'FALSE'

                        row = [uneNoteSite.periode, uneNoteSite.libelleMatiere, theValeur, theNoteSur, theCoef, uneNoteSite.typeDevoir, uneNoteSite.devoir, uneNoteSite.date, '=SI(ESTNUM(C' + str(googleNextRow) + ');C' + str(googleNextRow) + '/D' + str(googleNextRow) + '*20;NA())', '=I' + str(googleNextRow) + '*E' + str(googleNextRow) + '', '=SI(ESTNUM(I' + str(googleNextRow) + ');ET(VRAI;M' + str(googleNextRow) + ');FAUX)', '=GAUCHE(A' + str(googleNextRow) + ';4)',VRAI_NOTE]
                        try:
                            sheet_ongleNotes.insert_row(row, googleNextRow, 'USER_ENTERED')
                        except gspread.exceptions.APIError as argh:
                            print("Maximum d'ajout pour Google sheet - relancer dans 2 min")
                            print("api error : ", argh, file=sys.stderr)
                            inventaireNote = inventaireNote + "\n__api.error.max__"
                            erreurApiMax = True
                            break

                        inventaireNote = inventaireNote + "\n " + uneNoteSite.libelleMatiere.lower() + " " + str(theValeur) + "/" + str(theNoteSur) + " (" + str(theCoef) + ")"
                        if ( uneNoteSite.nonSignificatif == True ):
                            inventaireNote = inventaireNote + "_ns_"

                        googleNextRow = googleNextRow + 1
                        nbCreate = nbCreate + 1
                    else:
                        print("Note %s" % uneNoteSite.valeur, " @ déjà présente")
                print("Nombre de notes ajoutées pour ",elevePrenom," = ", nbCreate)
                telegram_message = telegram_message + "\n *"  + elevePrenom + "* :`" + str(nbCreate) + "`"
                if ( (nbCreate > 0) or (erreurApiMax) ):
                    telegram_message = telegram_message + inventaireNote
                break
        if ( not trouveEleve ):
            print(">> Eleve config non trouvé !!")
    print("Fin extraction.\nTotal nouvelles notes=" + str(compteurTotalNouvelleNote))

    if ( str(args.telegram) == "yes" ) :
        bot = telegram.Bot(token=str(args.token))
        bot.send_message(chat_id=str(args.chatid), text=telegram_message, parse_mode=telegram.ParseMode.MARKDOWN)

