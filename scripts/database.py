"""
Script de gestion de la base de données SQLite pour l'Observatoire de l'emploi territorial
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Base = declarative_base()

class Emploi(Base):
    """Modèle SQLAlchemy pour la table emploi"""
    __tablename__ = 'emploi'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code_departement = Column(String(10), nullable=False)
    nom_departement = Column(String(100), nullable=False)
    region = Column(String(100))
    annee = Column(Integer, nullable=False)
    trimestre = Column(Integer)
    emploi_total = Column(Integer)
    emploi_salarie = Column(Integer)
    emploi_non_salarie = Column(Integer)
    taux_chomage = Column(Float)
    nombre_entreprises = Column(Integer)
    creation_entreprises = Column(Integer)
    densite_entreprises = Column(Float)
    variation_emploi_total = Column(Float)
    variation_taux_chomage = Column(Float)
    variation_entreprises = Column(Float)
    performance_emploi = Column(String(50))
    taux_emploi_salarie = Column(Float)
    emploi_par_entreprise = Column(Float)

class DatabaseManager:
    """Classe pour gérer la base de données SQLite"""
    
    def __init__(self, db_path: str = "emploi.db"):
        self.db_path = Path(db_path)
        self.engine = create_engine(f'sqlite:///{self.db_path}', echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
    def create_database(self) -> bool:
        """
        Crée la base de données et les tables
        
        Returns:
            True si succès, False sinon
        """
        try:
            logger.info("Création de la base de données")
            
            # Créer les tables
            Base.metadata.create_all(bind=self.engine)
            logger.info("Tables créées avec succès")
            
            # Créer des index pour optimiser les performances
            self.create_indexes()
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de la base de données: {e}")
            return False
    
    def create_indexes(self) -> None:
        """Crée des index pour optimiser les requêtes"""
        logger.info("Création des index")
        
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_emploi_departement ON emploi(code_departement);",
            "CREATE INDEX IF NOT EXISTS idx_emploi_annee ON emploi(annee);",
            "CREATE INDEX IF NOT EXISTS idx_emploi_region ON emploi(region);",
            "CREATE INDEX IF NOT EXISTS idx_emploi_dept_annee ON emploi(code_departement, annee);"
        ]
        
        with self.engine.connect() as conn:
            for query in index_queries:
                conn.execute(text(query))
            conn.commit()
        
        logger.info("Index créés avec succès")
    
    def load_clean_data(self, csv_path: str = "data/clean/emploi_clean.csv") -> Optional[pd.DataFrame]:
        """
        Charge les données nettoyées depuis le fichier CSV
        
        Args:
            csv_path: Chemin vers le fichier CSV
            
        Returns:
            DataFrame avec les données ou None en cas d'erreur
        """
        try:
            if not Path(csv_path).exists():
                logger.error(f"Le fichier {csv_path} n'existe pas")
                return None
            
            logger.info(f"Chargement des données depuis {csv_path}")
            df = pd.read_csv(csv_path, encoding='utf-8')
            logger.info(f"Données chargées: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            return None
    
    def insert_data(self, df: pd.DataFrame) -> bool:
        """
        Insère les données dans la base de données
        
        Args:
            df: DataFrame à insérer
            
        Returns:
            True si succès, False sinon
        """
        try:
            logger.info("Insertion des données dans la base de données")
            
            # Insérer les données en utilisant pandas to_sql
            df.to_sql('emploi', self.engine, if_exists='replace', index=False)
            
            logger.info(f"{len(df)} lignes insérées avec succès")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'insertion des données: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Optional[pd.DataFrame]:
        """
        Exécute une requête SQL et retourne les résultats
        
        Args:
            query: Requête SQL
            params: Paramètres de la requête
            
        Returns:
            DataFrame avec les résultats ou None en cas d'erreur
        """
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                # Convertir en DataFrame
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
                
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            return None
    
    def get_departements(self) -> List[str]:
        """
        Récupère la liste des départements
        
        Returns:
            Liste des codes département
        """
        query = "SELECT DISTINCT code_departement FROM emploi ORDER BY code_departement"
        result = self.execute_query(query)
        return result['code_departement'].tolist() if result is not None else []
    
    def get_regions(self) -> List[str]:
        """
        Récupère la liste des régions
        
        Returns:
            Liste des régions
        """
        query = "SELECT DISTINCT region FROM emploi WHERE region IS NOT NULL ORDER BY region"
        result = self.execute_query(query)
        return result['region'].tolist() if result is not None else []
    
    def get_annees(self) -> List[int]:
        """
        Récupère la liste des années disponibles
        
        Returns:
            Liste des années
        """
        query = "SELECT DISTINCT annee FROM emploi ORDER BY annee"
        result = self.execute_query(query)
        return result['annee'].tolist() if result is not None else []
    
    def get_kpi_global(self) -> Dict[str, Any]:
        """
        Calcule les KPI globaux
        
        Returns:
            Dictionnaire avec les KPI
        """
        queries = {
            'total_emploi': "SELECT SUM(emploi_total) as total FROM emploi WHERE annee = (SELECT MAX(annee) FROM emploi)",
            'total_entreprises': "SELECT SUM(nombre_entreprises) as total FROM emploi WHERE annee = (SELECT MAX(annee) FROM emploi)",
            'taux_chomage_moyen': "SELECT AVG(taux_chomage) as moyenne FROM emploi WHERE annee = (SELECT MAX(annee) FROM emploi)",
            'variation_emploi': """
                SELECT AVG(variation_emploi_total) as variation 
                FROM emploi 
                WHERE annee = (SELECT MAX(annee) FROM emploi) 
                AND variation_emploi_total IS NOT NULL
            """
        }
        
        kpi = {}
        for key, query in queries.items():
            result = self.execute_query(query)
            if result is not None and len(result) > 0:
                value = result.iloc[0, 0]
                if pd.notna(value):
                    if key == 'taux_chomage_moyen' or key == 'variation_emploi':
                        kpi[key] = round(float(value), 2)
                    else:
                        kpi[key] = int(value)
                else:
                    kpi[key] = 0
            else:
                kpi[key] = 0
        
        return kpi
    
    def get_evolution_temporelle(self, departement: Optional[str] = None, 
                               region: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Récupère les données d'évolution temporelle
        
        Args:
            departement: Filtre par département (optionnel)
            region: Filtre par région (optionnel)
            
        Returns:
            DataFrame avec les données d'évolution
        """
        query = """
            SELECT annee, 
                   AVG(emploi_total) as emploi_total_moyen,
                   AVG(taux_chomage) as taux_chomage_moyen,
                   AVG(nombre_entreprises) as entreprises_moyennes,
                   AVG(variation_emploi_total) as variation_moyenne
            FROM emploi
            WHERE 1=1
        """
        
        params = {}
        if departement:
            query += " AND code_departement = :dept"
            params['dept'] = departement
        if region:
            query += " AND region = :region"
            params['region'] = region
            
        query += " GROUP BY annee ORDER BY annee"
        
        return self.execute_query(query, params) if params else self.execute_query(query)
    
    def get_classement_departements(self, annee: Optional[int] = None, 
                                   metric: str = 'emploi_total') -> Optional[pd.DataFrame]:
        """
        Récupère le classement des départements selon une métrique
        
        Args:
            annee: Année de filtrage (optionnel)
            metric: Métrique de classement
            
        Returns:
            DataFrame avec le classement
        """
        if annee is None:
            annee_query = "SELECT MAX(annee) FROM emploi"
            result = self.execute_query(annee_query)
            if result is not None:
                annee = result.iloc[0, 0]
        
        query = f"""
            SELECT code_departement, 
                   nom_departement,
                   {metric},
                   RANK() OVER (ORDER BY {metric} DESC) as rang
            FROM emploi 
            WHERE annee = :annee AND {metric} IS NOT NULL
            ORDER BY {metric} DESC
            LIMIT 20
        """
        
        return self.execute_query(query, {'annee': annee})
    
    def get_stats_region(self, region: str) -> Optional[pd.DataFrame]:
        """
        Récupère les statistiques pour une région
        
        Args:
            region: Nom de la région
            
        Returns:
            DataFrame avec les statistiques régionales
        """
        query = """
            SELECT annee,
                   COUNT(DISTINCT code_departement) as nb_departements,
                   SUM(emploi_total) as emploi_total,
                   AVG(taux_chomage) as taux_chomage_moyen,
                   SUM(nombre_entreprises) as total_entreprises
            FROM emploi
            WHERE region = :region
            GROUP BY annee
            ORDER BY annee
        """
        
        return self.execute_query(query, {'region': region})
    
    def initialize_database(self) -> bool:
        """
        Initialise complètement la base de données
        
        Returns:
            True si succès, False sinon
        """
        logger.info("Initialisation de la base de données")
        
        # Créer la base de données
        if not self.create_database():
            return False
        
        # Charger les données nettoyées
        df = self.load_clean_data()
        if df is None:
            return False
        
        # Insérer les données
        if not self.insert_data(df):
            return False
        
        logger.info("Base de données initialisée avec succès")
        return True
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Récupère des informations sur la base de données
        
        Returns:
            Dictionnaire avec les informations
        """
        info = {}
        
        # Nombre de lignes
        result = self.execute_query("SELECT COUNT(*) as count FROM emploi")
        info['total_rows'] = result.iloc[0, 0] if result is not None else 0
        
        # Nombre de départements
        result = self.execute_query("SELECT COUNT(DISTINCT code_departement) as count FROM emploi")
        info['total_departements'] = result.iloc[0, 0] if result is not None else 0
        
        # Période couverte
        result = self.execute_query("SELECT MIN(annee), MAX(annee) FROM emploi")
        if result is not None:
            info['periode'] = f"{result.iloc[0, 0]} - {result.iloc[0, 1]}"
        
        # Taille du fichier
        if self.db_path.exists():
            info['taille_fichier'] = f"{self.db_path.stat().st_size / 1024 / 1024:.2f} MB"
        
        return info

def main():
    """Fonction principale"""
    db_manager = DatabaseManager()
    
    # Initialiser la base de données
    if db_manager.initialize_database():
        logger.info("Initialisation réussie")
        
        # Afficher des informations
        info = db_manager.get_database_info()
        logger.info("Informations sur la base de données:")
        for key, value in info.items():
            logger.info(f"- {key}: {value}")
        
        # Tester quelques requêtes
        logger.info("\nTest des requêtes:")
        
        # KPI globaux
        kpi = db_manager.get_kpi_global()
        logger.info(f"KPI globaux: {kpi}")
        
        # Liste des départements
        departements = db_manager.get_departements()[:5]  # Premiers 5
        logger.info(f"Exemple de départements: {departements}")
        
    else:
        logger.error("L'initialisation a échoué")

if __name__ == "__main__":
    main()
