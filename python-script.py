import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pytz
import os
import logging
from shopify import Shopify, PaginatedIterator, GraphQL

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chemin vers le fichier credentials.json
CREDENTIALS_PATH = "credentials.json"

def load_credentials():
    """
    Charge les identifiants depuis un fichier JSON ou des variables d'environnement.
    
    Returns:
        dict: Dictionnaire contenant les identifiants.
    """
    try:
        # Essayer de charger depuis le fichier JSON
        if os.path.exists(CREDENTIALS_PATH):
            with open(CREDENTIALS_PATH, "r") as f:
                return json.load(f)
        else:
            # Charger depuis les variables d'environnement
            return {
                "google_sheets": {
                    "type": "service_account",
                    "project_id": os.getenv("GOOGLE_SHEETS_PROJECT_ID"),
                    "private_key_id": os.getenv("GOOGLE_SHEETS_PRIVATE_KEY_ID"),
                    "private_key": os.getenv("GOOGLE_SHEETS_PRIVATE_KEY").replace("\\n", "\n"),
                    "client_email": os.getenv("GOOGLE_SHEETS_CLIENT_EMAIL"),
                    "client_id": os.getenv("GOOGLE_SHEETS_CLIENT_ID"),
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": os.getenv("GOOGLE_SHEETS_CLIENT_X509_CERT_URL")
                },
                "amazon": {
                    "access_token": os.getenv("AMAZON_ACCESS_TOKEN"),
                    "marketplace_id": os.getenv("AMAZON_MARKETPLACE_ID")
                },
                "shopify": [
                    {
                        "access_token": os.getenv("SHOPIFY1_ACCESS_TOKEN")
                    },
                    {
                        "access_token": os.getenv("SHOPIFY2_ACCESS_TOKEN")
                    }
                ]
            }
    except Exception as e:
        logger.error(f"Erreur lors du chargement des identifiants : {e}")
        raise

class EcommerceDataProcessor:
    def __init__(self, credentials):
        """
        Initialise le processeur de données e-commerce.
        
        Args:
            credentials (dict): Dictionnaire contenant les clés API et autres informations d'authentification
        """
        self.credentials = credentials
        self.amazon_data = None
        self.shopify_data = []  # Liste pour contenir les données Shopify
        self.sku_mapping = None
        self.initialize_google_sheets()
        
    def initialize_google_sheets(self):
        """Initialise la connexion à Google Sheets"""
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials["google_sheets"], scope)
            self.gs_client = gspread.authorize(creds)
            logger.info("Connexion à Google Sheets établie avec succès")
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation de Google Sheets: {e}")
            raise
            
    def load_sku_mapping(self, sheet_id):
        """
        Charge les données de mapping des SKUs depuis Google Sheets
        
        Args:
            sheet_id (str): ID de la feuille Google Sheets contenant le mapping des SKUs
            
        Returns:
            pandas.DataFrame: DataFrame contenant le mapping des SKUs
        """
        try:
            sheet = self.gs_client.open_by_key(sheet_id).worksheet("Sheet1")
            data = sheet.get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0])
            self.sku_mapping = df
            logger.info(f"Mapping des SKUs chargé avec succès: {len(df)} entrées")
            return df
        except Exception as e:
            logger.error(f"Erreur lors du chargement du mapping des SKUs: {e}")
            raise
            
    def fetch_amazon_data(self, start_date, end_date):
        """
        Récupère les données de vente d'Amazon Seller Central
        
        Args:
            start_date (str): Date de début au format YYYY-MM-DD
            end_date (str): Date de fin au format YYYY-MM-DD
            
        Returns:
            pandas.DataFrame: DataFrame contenant les données de vente Amazon
        """
        try:
            # Configuration des headers et paramètres d'authentification Amazon
            headers = {
                "x-amz-access-token": self.credentials["amazon"]["access_token"],
                "Content-Type": "application/json"
            }
            
            # Point de terminaison pour les rapports Amazon SP-API
            url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports"
            
            # Créer une demande de rapport
            report_data = {
                "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
                "dataStartTime": f"{start_date}T00:00:00Z",
                "dataEndTime": f"{end_date}T23:59:59Z",
                "marketplaceIds": [self.credentials["amazon"]["marketplace_id"]]
            }
            
            # Simulons des données pour le prototype
            mock_data = self.generate_mock_amazon_data(start_date, end_date)
            self.amazon_data = mock_data
            
            logger.info(f"Données Amazon récupérées avec succès: {len(mock_data)} commandes")
            return mock_data
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données Amazon: {e}")
            raise
            
    def generate_mock_amazon_data(self, start_date, end_date):
        """
        Génère des données Amazon simulées pour le prototype
        
        Args:
            start_date (str): Date de début
            end_date (str): Date de fin
            
        Returns:
            pandas.DataFrame: DataFrame contenant des données Amazon simulées
        """
        # Conversion des dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Génération d'une liste de dates
        date_range = [start + timedelta(days=x) for x in range((end-start).days + 1)]
        
        # Produits Amazon extraits du fichier Global SKU List
        amazon_products = [
            {"name": "Advanced OG", "asin": "B09VY25KD8", "price": 149.99},
            {"name": "Advanced PL", "asin": "B09ZF8LVBK", "price": 169.99},
            {"name": "Standard", "asin": "B09VYXL17W", "price": 99.99},
            {"name": "Basic", "asin": "B09VY2HGVK", "price": 49.99}
        ]
        
        # Génération de données aléatoires
        rows = []
        for date in date_range:
            for product in amazon_products:
                # Nombre aléatoire de ventes par jour et par produit (entre 0 et 10)
                quantity = np.random.randint(0, 11)
                if quantity > 0:
                    rows.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "order_id": f"AMZ-{np.random.randint(100000, 999999)}",
                        "product_name": product["name"],
                        "asin": product["asin"],
                        "quantity": quantity,
                        "price": product["price"],
                        "total": quantity * product["price"]
                    })
        
        return pd.DataFrame(rows)
    
    def fetch_shopify_data(self, shop_url, start_date, end_date):
        """
        Récupère les données de vente de Shopify
        
        Args:
            shop_url (str): URL de la boutique Shopify
            start_date (str): Date de début au format YYYY-MM-DD
            end_date (str): Date de fin au format YYYY-MM-DD
            
        Returns:
            pandas.DataFrame: DataFrame contenant les données de vente Shopify
        """
        try:
            # Configuration de l'API Shopify
            api_key = self.credentials["shopify"]["api_key"]
            secret_key = self.credentials["shopify"]["secret_key"]
            
            # Point de terminaison pour les commandes Shopify
            url = f"https://{api_key}:{secret_key}@{shop_url}/admin/api/2024-01/orders.json"
            
            params = {
                "status": "any",
                "created_at_min": f"{start_date}T00:00:00Z",
                "created_at_max": f"{end_date}T23:59:59Z",
                "limit": 250  # Maximum autorisé par Shopify
            }
            
            # Récupération des données
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            # Conversion des données en DataFrame
            orders = response.json().get("orders", [])
            rows = []
            for order in orders:
                for item in order.get("line_items", []):
                    rows.append({
                        "date": order["created_at"][:10],
                        "order_id": order["id"],
                        "product_name": item["title"],
                        "sku": item["sku"],
                        "quantity": item["quantity"],
                        "price": float(item["price"]),
                        "total": float(item["price"]) * item["quantity"]
                    })
            
            df = pd.DataFrame(rows)
            self.shopify_data.append(df)
            
            logger.info(f"Données Shopify récupérées avec succès: {len(df)} commandes")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données Shopify: {e}")
            raise
    
    def generate_mock_shopify_data(self, start_date, end_date, shop_index):
        """
        Génère des données Shopify simulées pour le prototype
        
        Args:
            start_date (str): Date de début
            end_date (str): Date de fin
            shop_index (int): Index de la boutique Shopify
            
        Returns:
            pandas.DataFrame: DataFrame contenant des données Shopify simulées
        """
        # Conversion des dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Génération d'une liste de dates
        date_range = [start + timedelta(days=x) for x in range((end-start).days + 1)]
        
        # Produits Shopify extraits du fichier Global SKU List
        shopify_products = [
            {"name": "Advanced Bundle", "sku": "Advanced Bundle", "price": 149.99},
            {"name": "Advanced Bundle + WiFi", "sku": "Advanced Bundle + WiFi", "price": 189.99},
            {"name": "Advanced Bundle + Remote", "sku": "Advanced Bundle + Remote", "price": 179.99},
            {"name": "Expert Bundle", "sku": "Expert Bundle", "price": 199.99}
        ]
        
        # Ajout de variation pour la deuxième boutique
        if shop_index == 1:
            for product in shopify_products:
                product["price"] = product["price"] * 0.9  # Prix légèrement différents
        
        # Génération de données aléatoires
        rows = []
        for date in date_range:
            for product in shopify_products:
                # Nombre aléatoire de ventes par jour et par produit (entre 0 et 8)
                quantity = np.random.randint(0, 9)
                if quantity > 0:
                    rows.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "order_id": f"SHOP{shop_index+1}-{np.random.randint(100000, 999999)}",
                        "product_name": product["name"],
                        "sku": product["sku"],
                        "quantity": quantity,
                        "price": product["price"],
                        "total": quantity * product["price"]
                    })
        
        return pd.DataFrame(rows)
    
    def process_bundle_logic(self, df):
        """
        Applique la logique de bundle aux données
        
        Args:
            df (pandas.DataFrame): DataFrame contenant les données de vente
            
        Returns:
            pandas.DataFrame: DataFrame avec la logique de bundle appliquée
        """
        try:
            # Cette fonction devrait implémenter la logique complexe de bundle mentionnée dans les spécifications
            # Par exemple, 1 unité de "Grape" = 3 unités de "Apple" + 1 unité de "Orange" + 1 unité de "Pear"
            
            # Pour le prototype, nous allons simplement ajouter une colonne pour marquer les bundles
            df['is_bundle'] = df['product_name'].str.contains('Bundle')
            
            # Ajouter une colonne pour indiquer si le produit a une option WiFi
            df['has_wifi'] = df['product_name'].str.contains('WiFi')
            
            # Ajouter une colonne pour indiquer si le produit a une option Remote
            df['has_remote'] = df['product_name'].str.contains('Remote')
            
            logger.info(f"Logique de bundle appliquée avec succès sur {len(df)} entrées")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'application de la logique de bundle: {e}")
            raise
    
    def generate_sales_rate_report(self, start_date, end_date):
        """
        Génère le rapport de taux de vente
        
        Args:
            start_date (str): Date de début au format YYYY-MM-DD
            end_date (str): Date de fin au format YYYY-MM-DD
            
        Returns:
            pandas.DataFrame: DataFrame contenant le rapport de taux de vente
        """
        try:
            # Vérifier que nous avons des données
            if self.amazon_data is None or len(self.shopify_data) == 0:
                raise ValueError("Les données n'ont pas été chargées. Appelez fetch_amazon_data() et fetch_shopify_data() d'abord.")
            
            # Création du template de rapport
            report = pd.DataFrame()
            report['Start Date'] = [start_date]
            report['End Date'] = [end_date]
            
            # Préparation des colonnes du rapport selon le modèle Sales Rates Report.xlsx
            product_data = []
            
            # Récupération de la liste des produits à partir du mapping des SKUs
            if self.sku_mapping is not None:
                products = self.sku_mapping[self.sku_mapping['Product Name'].notna()]['Product Name'].unique()
            else:
                # Si le mapping n'est pas disponible, utilisons une liste de produits par défaut
                products = [
                    "Advanced Bundle",
                    "Standard",
                    "Basic",
                    "Basic+ Bundle"
                ]
            
            # Pour chaque produit, calculer les quantités vendues sur chaque plateforme
            for product in products:
                row = {'Product Name': product}
                
                # Amazon
                if self.amazon_data is not None:
                    amazon_qty = 0
                    if 'product_name' in self.amazon_data.columns:
                        amazon_qty = self.amazon_data[self.amazon_data['product_name'].str.contains(product, na=False)]['quantity'].sum()
                    row['Amazon Product'] = product
                    row['Amazon ASIN'] = ''  # À remplir avec les données réelles du mapping
                    row['QTY Sold (Amazon)'] = amazon_qty
                
                # Shopify (premier magasin)
                if len(self.shopify_data) > 0:
                    shopify1_qty = 0
                    if 'product_name' in self.shopify_data[0].columns:
                        shopify1_qty = self.shopify_data[0][self.shopify_data[0]['product_name'].str.contains(product, na=False)]['quantity'].sum()
                    row['Shopify SKU'] = product
                    row['QTY Sold (Shopify1)'] = shopify1_qty
                
                # Shopify (deuxième magasin, si disponible)
                if len(self.shopify_data) > 1:
                    shopify2_qty = 0
                    if 'product_name' in self.shopify_data[1].columns:
                        shopify2_qty = self.shopify_data[1][self.shopify_data[1]['product_name'].str.contains(product, na=False)]['quantity'].sum()
                    row['QTY Sold (Shopify2)'] = shopify2_qty
                
                product_data.append(row)
            
            # Conversion en DataFrame
            product_df = pd.DataFrame(product_data)
            
            logger.info(f"Rapport de taux de vente généré avec succès: {len(product_df)} produits")
            return product_df
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du rapport de taux de vente: {e}")
            raise
    
    def generate_weekly_sales_report(self, current_week_start, current_week_end):
        """
        Génère le rapport hebdomadaire des ventes
        
        Args:
            current_week_start (str): Date de début de la semaine courante au format YYYY-MM-DD
            current_week_end (str): Date de fin de la semaine courante au format YYYY-MM-DD
            
        Returns:
            pandas.DataFrame: DataFrame contenant le rapport hebdomadaire des ventes
        """
        try:
            # Vérifier que nous avons des données
            if self.amazon_data is None or len(self.shopify_data) == 0:
                raise ValueError("Les données n'ont pas été chargées. Appelez fetch_amazon_data() et fetch_shopify_data() d'abord.")
            
            # Conversion des dates
            current_week_start_dt = datetime.strptime(current_week_start, "%Y-%m-%d")
            current_week_end_dt = datetime.strptime(current_week_end, "%Y-%m-%d")
            
            # Calcul des périodes
            prior_week_start = (current_week_start_dt - timedelta(days=7)).strftime("%Y-%m-%d")
            prior_week_end = (current_week_end_dt - timedelta(days=7)).strftime("%Y-%m-%d")
            
            prior_year_start = (current_week_start_dt - timedelta(days=365)).strftime("%Y-%m-%d")
            prior_year_end = (current_week_end_dt - timedelta(days=365)).strftime("%Y-%m-%d")
            
            last_4_weeks_start = (current_week_start_dt - timedelta(days=28)).strftime("%Y-%m-%d")
            
            # Année en cours
            current_year = current_week_start_dt.year
            ytd_start = f"{current_year}-01-01"
            ytd_end = current_week_end
            
            # Année précédente
            prior_year = current_year - 1
            prior_ytd_start = f"{prior_year}-01-01"
            prior_ytd_end = f"{prior_year}-{current_week_end_dt.month:02d}-{current_week_end_dt.day:02d}"
            
            # Création du template de rapport
            iso_week = current_week_start_dt.isocalendar()[1]
            report_header = {
                'ISOWEEK': [f"ISOWEEK {iso_week} Dashboard"],
                'NOTE': ["Sales Ex VAT (VAT is on all products) - Ex returns."]
            }
            
            # Colonnes du rapport
            columns = [
                'Platform',
                'Last week',
                'Prior Week',
                'Prior Year',
                'Last 4 weeks',
                'YTD',
                'Prior YTD',
                'WOW',  # Week on Week growth
                'YOY',  # Year on Year growth
                'YOY YTD'  # Year on Year Year to Date growth
            ]
            
            # Initialisation du rapport
            report_data = []
            
            # Fonction pour calculer les ventes totales pour une période donnée
            def calculate_sales(data, start_date, end_date):
                if 'date' in data.columns and 'total' in data.columns:
                    filtered = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
                    return filtered['total'].sum()
                return 0
            
            # Données pour Amazon
            amazon_current_week = calculate_sales(self.amazon_data, current_week_start, current_week_end)
            amazon_prior_week = calculate_sales(self.amazon_data, prior_week_start, prior_week_end)
            amazon_prior_year = calculate_sales(self.amazon_data, prior_year_start, prior_year_end)
            amazon_last_4_weeks = calculate_sales(self.amazon_data, last_4_weeks_start, current_week_end)
            amazon_ytd = calculate_sales(self.amazon_data, ytd_start, ytd_end)
            amazon_prior_ytd = calculate_sales(self.amazon_data, prior_ytd_start, prior_ytd_end)
            
            # Calcul des taux de croissance pour Amazon
            amazon_wow = (amazon_current_week - amazon_prior_week) / amazon_prior_week if amazon_prior_week > 0 else 0
            amazon_yoy = (amazon_current_week - amazon_prior_year) / amazon_prior_year if amazon_prior_year > 0 else 0
            amazon_yoy_ytd = (amazon_ytd - amazon_prior_ytd) / amazon_prior_ytd if amazon_prior_ytd > 0 else 0
            
            # Ajout des données Amazon au rapport
            report_data.append({
                'Platform': 'AMAZON',
                'Last week': f"£{amazon_current_week:.2f}",
                'Prior Week': f"£{amazon_prior_week:.2f}",
                'Prior Year': f"£{amazon_prior_year:.2f}",
                'Last 4 weeks': f"£{amazon_last_4_weeks:.2f}",
                'YTD': f"£{amazon_ytd:.2f}",
                'Prior YTD': f"£{amazon_prior_ytd:.2f}",
                'WOW': amazon_wow,
                'YOY': amazon_yoy,
                'YOY YTD': amazon_yoy_ytd
            })
            
            # Données pour chaque boutique Shopify
            for i, shopify_data in enumerate(self.shopify_data):
                shopify_current_week = calculate_sales(shopify_data, current_week_start, current_week_end)
                shopify_prior_week = calculate_sales(shopify_data, prior_week_start, prior_week_end)
                shopify_prior_year = calculate_sales(shopify_data, prior_year_start, prior_year_end)
                shopify_last_4_weeks = calculate_sales(shopify_data, last_4_weeks_start, current_week_end)
                shopify_ytd = calculate_sales(shopify_data, ytd_start, ytd_end)
                shopify_prior_ytd = calculate_sales(shopify_data, prior_ytd_start, prior_ytd_end)
                
                # Calcul des taux de croissance pour Shopify
                shopify_wow = (shopify_current_week - shopify_prior_week) / shopify_prior_week if shopify_prior_week > 0 else 0
                shopify_yoy = (shopify_current_week - shopify_prior_year) / shopify_prior_year if shopify_prior_year > 0 else 0
                shopify_yoy_ytd = (shopify_ytd - shopify_prior_ytd) / shopify_prior_ytd if shopify_prior_ytd > 0 else 0
                
                # Ajout des données Shopify au rapport
                report_data.append({
                    'Platform': f"SHOPIFY{i+1}",
                    'Last week': f"£{shopify_current_week:.2f}",
                    'Prior Week': f"£{shopify_prior_week:.2f}",
                    'Prior Year': f"£{shopify_prior_year:.2f}",
                    'Last 4 weeks': f"£{shopify_last_4_weeks:.2f}",
                    'YTD': f"£{shopify_ytd:.2f}",
                    'Prior YTD': f"£{shopify_prior_ytd:.2f}",
                    'WOW': shopify_wow,
                    'YOY': shopify_yoy,
                    'YOY YTD': shopify_yoy_ytd
                })
            
            # Conversion en DataFrame
            report_df = pd.DataFrame(report_data)
            
            logger.info(f"Rapport hebdomadaire des ventes généré avec succès: {len(report_df)} entrées")
            return report_df
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du rapport hebdomadaire des ventes: {e}")
            raise
    
    def export_to_google_sheets(self, df, sheet_id, worksheet_name):
        """
        Exporte un DataFrame vers Google Sheets
        
        Args:
            df (pandas.DataFrame): DataFrame à exporter
            sheet_id (str): ID de la feuille Google Sheets
            worksheet_name (str): Nom de la feuille de calcul
            
        Returns:
            bool: True si l'exportation a réussi, False sinon
        """
        try:
            # Ouvrir la feuille Google Sheets
            sheet = self.gs_client.open_by_key(sheet_id)
            
            # Vérifier si la feuille de calcul existe, sinon la créer
            try:
                worksheet = sheet.worksheet(worksheet_name)
            except:
                worksheet = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
            
            # Effacer le contenu existant
            worksheet.clear()
            
            # Convertir le DataFrame en liste de listes
            values = [df.columns.tolist()] + df.values.tolist()
            
            # Mettre à jour la feuille
            worksheet.update(values)
            
            logger.info(f"Données exportées avec succès vers {worksheet_name} dans Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'exportation vers Google Sheets: {e}")
            return False

    def create_dashboard_data(self):
        """
        Crée les données pour le tableau de bord
        
        Returns:
            pandas.DataFrame: DataFrame contenant les données pour le tableau de bord
        """
        try:
            # Vérifier que nous avons des données
            if self.amazon_data is None or len(self.shopify_data) == 0:
                raise ValueError("Les données n'ont pas été chargées. Appelez fetch_amazon_data() et fetch_shopify_data() d'abord.")
            
            # Métriques pour le tableau de bord
            metrics = {}
            
            # Date de la dernière mise à jour
            metrics['derniere_mise_a_jour'] = datetime.now(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S")
            
            # Calcul des ventes totales par plateforme
            amazon_total = self.amazon_data['total'].sum() if 'total' in self.amazon_data.columns else 0
            metrics['ventes_amazon'] = amazon_total
            
            shopify1_total = self.shopify_data[0]['total'].sum() if len(self.shopify_data) > 0 and 'total' in self.shopify_data[0].columns else 0
            metrics['ventes_shopify1'] = shopify1_total
            
            shopify2_total = self.shopify_data[1]['total'].sum() if len(self.shopify_data) > 1 and 'total' in self.shopify_data[1].columns else 0
            metrics['ventes_shopify2'] = shopify2_total
            
            metrics['ventes_totales'] = amazon_total + shopify1_total + shopify2_total
            
            # Calcul des ventes par produit (top 5)
            all_product_sales = []
            
            # Amazon
            if 'product_name' in self.amazon_data.columns and 'total' in self.amazon_data.columns:
                amazon_by_product = self.amazon_data.groupby('product_name')['total'].sum().reset_index()
                amazon_by_product['platform'] = 'Amazon'
                all_product_sales.append(amazon_by_product)
            
            # Shopify1
            if len(self.shopify_data) > 0 and 'product_name' in self.shopify_data[0].columns and 'total' in self.shopify_data[0].columns:
                shopify1_by_product = self.shopify_data[0].groupby('product_name')['total'].sum().reset_index()
                shopify1_by_product['platform'] = 'Shopify1'
                all_product_sales.append(shopify1_by_product)
            
            # Shopify2
            if len(self.shopify_data) > 1 and 'product_name' in self.shopify_data[1].columns and 'total' in self.shopify_data[1].columns:
                shopify2_by_product = self.shopify_data[1].groupby('product_name')['total'].sum().reset_index()
                shopify2_by_product['platform'] = 'Shopify2'
                all_product_sales.append(shopify2_by_product)
            
            # Combinaison des données produit
            if all_product_sales:
                all_products_df = pd.concat(all_product_sales)
                top_products = all_products_df.sort_values('total', ascending=False).head(5)
                metrics['top_produits'] = top_products.to_dict('records')
            else:
                metrics['top_produits'] = []
            
            # Calcul des ventes par jour (dernière semaine)
            today = datetime.now().strftime("%Y-%m-%d")
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            daily_sales = []
            
            # Amazon
            if 'date' in self.amazon_data.columns and 'total' in self.amazon_data.columns:
                amazon_daily = self.amazon_data[(self.amazon_data['date'] >= week_ago) & (self.amazon_data['date'] <= today)]
                amazon_daily = amazon_daily.groupby('date')['total'].sum().reset_index()
                amazon_daily['platform'] = 'Amazon'
                daily_sales.append(amazon_daily)
            
            # Shopify1
            if len(self.shopify_data) > 0 and 'date' in self.shopify_data[0].columns and 'total' in self.shopify_data[0].columns:
                shopify1_daily = self.shopify_data[0][(self.shopify_data[0]['date'] >= week_ago) & (self.shopify_data[0]['date'] <= today)]
                shopify1_daily = shopify1_daily.groupby('date')['total'].sum().reset_index()
                shopify1_daily['platform'] = 'Shopify1'
                daily_sales.append(shopify1_daily)
            
            # Shopify2
            if len(self.shopify_data) > 1 and 'date' in self.shopify_data[1].columns and 'total' in self.shopify_data[1].columns:
                shopify2_daily = self.shopify_data[1][(self.shopify_data[1]['date'] >= week_ago) & (self.shopify_data[1]['date'] <= today)]
                shopify2_daily = shopify2_daily.groupby('date')['total'].sum().reset_index()
                shopify2_daily['platform'] = 'Shopify2'
                daily_sales.append(shopify2_daily)
            
            # Combinaison des données quotidiennes
            if daily_sales:
                all_daily_sales = pd.concat(daily_sales)
                metrics['ventes_quotidiennes'] = all_daily_sales.to_dict('records')
            else:
                metrics['ventes_quotidiennes'] = []
            
            # Conversion en DataFrame pour l'exportation
            dashboard_df = pd.DataFrame([metrics])
            
            logger.info("Données du tableau de bord créées avec succès")
            return dashboard_df
            
        except Exception as e:
            logger.error(f"Erreur lors de la création des données du tableau de bord: {e}")
            raise
    
    def run_full_pipeline(self, start_date, end_date, sku_mapping_sheet_id, sales_report_sheet_id):
        """
        Exécute le pipeline complet de traitement des données
        
        Args:
            start_date (str): Date de début au format YYYY-MM-DD
            end_date (str): Date de fin au format YYYY-MM-DD
            sku_mapping_sheet_id (str): ID de la feuille Google Sheets contenant le mapping des SKUs
            sales_report_sheet_id (str): ID de la feuille Google Sheets pour le rapport de ventes
            
        Returns:
            bool: True si l'exécution a réussi, False sinon
        """
        try:
            logger.info(f"Démarrage du pipeline pour la période du {start_date} au {end_date}")
            
            # Étape 1: Chargement du mapping des SKUs
            self.load_sku_mapping(sku_mapping_sheet_id)
            
            # Étape 2: Récupération des données Amazon
            self.fetch_amazon_data(start_date, end_date)
            
            # Étape 3: Récupération des données Shopify (deux boutiques)
            self.fetch_shopify_data("shop1.myshopify.com", start_date, end_date, 0)
            self.fetch_shopify_data("shop2.myshopify.com", start_date, end_date, 1)
            
            # Étape 4: Génération du rapport de taux de vente
            sales_rate_report = self.generate_sales_rate_report(start_date, end_date)
            
            # Étape 5: Génération du rapport hebdomadaire des ventes
            # Pour le rapport hebdomadaire, nous utilisons les 7 derniers jours à partir de end_date
            week_start = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
            weekly_sales_report = self.generate_weekly_sales_report(week_start, end_date)
            
            # Étape 6: Création des données pour le tableau de bord
            dashboard_data = self.create_dashboard_data()
            
            # Étape 7: Exportation des rapports vers Google Sheets
            self.export_to_google_sheets(sales_rate_report, sales_report_sheet_id, "Sales Rate Report")
            self.export_to_google_sheets(weekly_sales_report, sales_report_sheet_id, "Weekly Sales Report")
            self.export_to_google_sheets(dashboard_data, sales_report_sheet_id, "Dashboard Data")
            
            logger.info("Pipeline exécuté avec succès")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du pipeline: {e}")
            return False

# Exemple d'utilisation
if __name__ == "__main__":
    # Charger les identifiants
    credentials = load_credentials()
    
    # Initialisation du processeur de données
    processor = EcommerceDataProcessor(credentials)
    
    # Définition de la période
    today = datetime.now()
    end_date = today.strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # IDs des feuilles Google Sheets
    sku_mapping_sheet_id = "your-sku-mapping-sheet-id"
    sales_report_sheet_id = "your-sales-report-sheet-id"
    
    # Exécution du pipeline
    success = processor.run_full_pipeline(start_date, end_date, sku_mapping_sheet_id, sales_report_sheet_id)
    
    if success:
        print("Traitement des données e-commerce terminé avec succès!")
    else:
        print("Erreur lors du traitement des données e-commerce.")