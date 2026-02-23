"""
Script de nettoyage et de structuration des données pour l'Observatoire de l'emploi territorial
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Optional, Tuple

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    """Classe pour nettoyer et structurer les données d'emploi"""
    
    def __init__(self):
        self.raw_data_path = Path("data/raw/emploi_raw.csv")
        self.clean_data_path = Path("data/clean/emploi_clean.csv")
        
    def load_raw_data(self) -> Optional[pd.DataFrame]:
        """
        Charge les données brutes depuis le fichier CSV
        
        Returns:
            DataFrame avec les données brutes ou None en cas d'erreur
        """
        try:
            if not self.raw_data_path.exists():
                logger.error(f"Le fichier {self.raw_data_path} n'existe pas")
                return None
            
            logger.info(f"Chargement des données depuis {self.raw_data_path}")
            df = pd.read_csv(self.raw_data_path, encoding='utf-8')
            logger.info(f"Données chargées: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            return None
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie les noms des colonnes
        
        Args:
            df: DataFrame d'entrée
            
        Returns:
            DataFrame avec les noms de colonnes nettoyés
        """
        logger.info("Nettoyage des noms de colonnes")
        
        # Convertir en minuscules et remplacer les espaces par des underscores
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Supprimer les caractères spéciaux
        df.columns = df.columns.str.replace('[^a-zA-Z0-9_]', '', regex=True)
        
        logger.info(f"Nouvelles colonnes: {list(df.columns)}")
        return df
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Gère les valeurs manquantes
        
        Args:
            df: DataFrame d'entrée
            
        Returns:
            DataFrame avec les valeurs manquantes traitées
        """
        logger.info("Traitement des valeurs manquantes")
        
        # Afficher les informations sur les valeurs manquantes
        missing_info = df.isnull().sum()
        logger.info(f"Valeurs manquantes par colonne:\n{missing_info[missing_info > 0]}")
        
        # Pour les colonnes numériques, remplacer par la médiane
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if df[col].isnull().sum() > 0:
                median_value = df[col].median()
                df[col] = df[col].fillna(median_value)
                logger.info(f"Colonne {col}: valeurs manquantes remplacées par la médiane ({median_value})")
        
        # Pour les colonnes catégorielles, remplacer par le mode
        categorical_columns = df.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            if df[col].isnull().sum() > 0:
                mode_value = df[col].mode()[0] if not df[col].mode().empty else 'Inconnu'
                df[col] = df[col].fillna(mode_value)
                logger.info(f"Colonne {col}: valeurs manquantes remplacées par le mode ({mode_value})")
        
        return df
    
    def convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convertit les types de données appropriés
        
        Args:
            df: DataFrame d'entrée
            
        Returns:
            DataFrame avec les types de données convertis
        """
        logger.info("Conversion des types de données")
        
        # Conversion des types de base
        type_conversions = {
            'annee': 'int32',
            'trimestre': 'int32',
            'emploi_total': 'int32',
            'emploi_salarie': 'int32',
            'emploi_non_salarie': 'int32',
            'nombre_entreprises': 'int32',
            'creation_entreprises': 'int32',
            'taux_chomage': 'float32',
            'densite_entreprises': 'float32'
        }
        
        for col, dtype in type_conversions.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                    logger.info(f"Colonne {col} convertie en {dtype}")
                except Exception as e:
                    logger.warning(f"Impossible de convertir {col} en {dtype}: {e}")
        
        return df
    
    def create_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crée des indicateurs utiles
        
        Args:
            df: DataFrame d'entrée
            
        Returns:
            DataFrame avec les nouveaux indicateurs
        """
        logger.info("Création des indicateurs")
        
        # Trier par département et année pour les calculs de variation
        df = df.sort_values(['code_departement', 'annee'])
        
        # Calculer les variations annuelles par département
        df['variation_emploi_total'] = df.groupby('code_departement')['emploi_total'].pct_change() * 100
        df['variation_taux_chomage'] = df.groupby('code_departement')['taux_chomage'].diff()
        df['variation_entreprises'] = df.groupby('code_departement')['nombre_entreprises'].pct_change() * 100
        
        # Créer des catégories de performance
        df['performance_emploi'] = pd.cut(
            df['variation_emploi_total'],
            bins=[-np.inf, -5, 0, 5, np.inf],
            labels=['Fortement négative', 'Négative', 'Positive', 'Forte positive']
        )
        
        # Créer des indicateurs par région (regroupement de départements)
        # Mapping simple des régions
        region_mapping = {
            # Île-de-France
            '75': 'Île-de-France', '77': 'Île-de-France', '78': 'Île-de-France', 
            '91': 'Île-de-France', '92': 'Île-de-France', '93': 'Île-de-France', 
            '94': 'Île-de-France', '95': 'Île-de-France',
            # Auvergne-Rhône-Alpes
            '01': 'Auvergne-Rhône-Alpes', '03': 'Auvergne-Rhône-Alpes', '07': 'Auvergne-Rhône-Alpes',
            '15': 'Auvergne-Rhône-Alpes', '26': 'Auvergne-Rhône-Alpes', '38': 'Auvergne-Rhône-Alpes',
            '42': 'Auvergne-Rhône-Alpes', '43': 'Auvergne-Rhône-Alpes', '63': 'Auvergne-Rhône-Alpes',
            '69': 'Auvergne-Rhône-Alpes', '73': 'Auvergne-Rhône-Alpes', '74': 'Auvergne-Rhône-Alpes',
            # Autres régions (simplifié)
        }
        
        # Ajouter une colonne région (par défaut 'Autre' si non mappé)
        df['region'] = df['code_departement'].map(region_mapping).fillna('Autre')
        
        # Calculer le taux d'emploi salarié
        df['taux_emploi_salarie'] = (df['emploi_salarie'] / df['emploi_total'] * 100).round(2)
        
        # Calculer la densité d'emploi par entreprise
        df['emploi_par_entreprise'] = (df['emploi_total'] / df['nombre_entreprises']).round(2)
        
        logger.info("Indicateurs créés avec succès")
        return df
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, list]:
        """
        Valide la qualité des données
        
        Args:
            df: DataFrame à valider
            
        Returns:
            Tuple (bool, list): True si valide, liste des problèmes sinon
        """
        logger.info("Validation des données")
        problems = []
        
        # Vérifier les valeurs négatives inappropriées
        negative_columns = ['emploi_total', 'emploi_salarie', 'emploi_non_salarie', 'nombre_entreprises']
        for col in negative_columns:
            if col in df.columns and (df[col] < 0).any():
                problems.append(f"Valeurs négatives trouvées dans {col}")
        
        # Vérifier les taux qui devraient être entre 0 et 100
        rate_columns = ['taux_chomage', 'taux_emploi_salarie']
        for col in rate_columns:
            if col in df.columns:
                if (df[col] < 0).any() or (df[col] > 100).any():
                    problems.append(f"Valeurs hors limites dans {col}")
        
        # Vérifier la cohérence emploi_total = emploi_salarie + emploi_non_salarie
        if all(col in df.columns for col in ['emploi_total', 'emploi_salarie', 'emploi_non_salarie']):
            diff = df['emploi_total'] - (df['emploi_salarie'] + df['emploi_non_salarie'])
            if abs(diff).max() > 1:  # Tolérance de 1
                problems.append("Incohérence dans les totaux d'emploi")
        
        # Vérifier les doublons
        if df.duplicated().any():
            problems.append("Doublons trouvés dans les données")
        
        is_valid = len(problems) == 0
        
        if is_valid:
            logger.info("Validation réussie")
        else:
            logger.warning(f"Problèmes de validation: {problems}")
        
        return is_valid, problems
    
    def save_clean_data(self, df: pd.DataFrame) -> bool:
        """
        Sauvegarde les données nettoyées
        
        Args:
            df: DataFrame à sauvegarder
            
        Returns:
            True si succès, False sinon
        """
        try:
            # Créer le dossier clean s'il n'existe pas
            self.clean_data_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Sauvegarder les données
            df.to_csv(self.clean_data_path, index=False, encoding='utf-8')
            logger.info(f"Données nettoyées sauvegardées dans {self.clean_data_path}")
            
            # Afficher un résumé
            logger.info(f"Résumé des données nettoyées:")
            logger.info(f"- Nombre de lignes: {len(df)}")
            logger.info(f"- Nombre de colonnes: {len(df.columns)}")
            logger.info(f"- Période: {df['annee'].min()} - {df['annee'].max()}")
            logger.info(f"- Nombre de départements: {df['code_departement'].nunique()}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde: {e}")
            return False
    
    def clean_data(self) -> Optional[pd.DataFrame]:
        """
        Fonction principale de nettoyage des données
        
        Returns:
            DataFrame nettoyé ou None en cas d'erreur
        """
        logger.info("Début du nettoyage des données")
        
        # Charger les données brutes
        df = self.load_raw_data()
        if df is None:
            return None
        
        # Appliquer les étapes de nettoyage
        df = self.clean_column_names(df)
        df = self.handle_missing_values(df)
        df = self.convert_data_types(df)
        df = self.create_indicators(df)
        
        # Valider les données
        is_valid, problems = self.validate_data(df)
        if not is_valid:
            logger.warning("Problèmes de validation détectés, mais poursuite du traitement")
        
        # Sauvegarder les données nettoyées
        if self.save_clean_data(df):
            return df
        else:
            return None

def main():
    """Fonction principale"""
    cleaner = DataCleaner()
    
    # Lancer le nettoyage
    df_clean = cleaner.clean_data()
    
    if df_clean is not None:
        logger.info("Nettoyage terminé avec succès")
        
        # Afficher un aperçu des données nettoyées
        logger.info("\nAperçu des données nettoyées:")
        logger.info(df_clean.head().to_string())
        logger.info(f"\nTypes de données:\n{df_clean.dtypes}")
        
    else:
        logger.error("Le nettoyage a échoué")

if __name__ == "__main__":
    main()
