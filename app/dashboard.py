"""
Dashboard Streamlit pour l'Observatoire de l'emploi territorial
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer les modules
sys.path.append(str(Path(__file__).parent.parent))

from scripts.database import DatabaseManager

# Configuration de la page
st.set_page_config(
    page_title="Observatoire de l'emploi territorial",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS pour améliorer le style
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .kpi-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: bold;
        color: #2e7d32;
    }
    .kpi-label {
        font-size: 0.9rem;
        color: #666;
    }
</style>
""", unsafe_allow_html=True)

class Dashboard:
    """Classe principale du dashboard"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.initialize_data()
    
    def initialize_data(self):
        """Initialise les données et vérifie la connexion à la base"""
        try:
            # Vérifier si la base de données existe et est initialisée
            info = self.db_manager.get_database_info()
            if info['total_rows'] == 0:
                st.error("La base de données est vide. Veuillez d'abord exécuter les scripts de scraping et de nettoyage.")
                st.stop()
            
            # Charger les données de base
            self.departements = self.db_manager.get_departements()
            self.regions = self.db_manager.get_regions()
            self.annees = self.db_manager.get_annees()
            
        except Exception as e:
            st.error(f"Erreur lors de l'initialisation des données: {e}")
            st.stop()
    
    def render_header(self):
        """Affiche l'en-tête du dashboard"""
        st.markdown('<h1 class="main-header">📊 Observatoire de l\'emploi territorial</h1>', unsafe_allow_html=True)
        st.markdown("---")
    
    def render_sidebar(self):
        """Affiche la barre latérale avec les filtres"""
        st.sidebar.header("🔍 Filtres")
        
        # Filtre par territoire
        territoire_type = st.sidebar.selectbox(
            "Type de territoire",
            ["Tous", "Département", "Région"],
            index=0
        )
        
        self.selected_departement = None
        self.selected_region = None
        
        if territoire_type == "Département":
            # Ajouter une option pour tous les départements
            dept_options = ["Tous"] + self.departements
            selected = st.sidebar.selectbox("Département", dept_options)
            self.selected_departement = selected if selected != "Tous" else None
            
        elif territoire_type == "Région":
            # Ajouter une option pour toutes les régions
            region_options = ["Toutes"] + self.regions
            selected = st.sidebar.selectbox("Région", region_options)
            self.selected_region = selected if selected != "Toutes" else None
        
        # Filtre par période
        annee_min, annee_max = min(self.annees), max(self.annees)
        annee_range = st.sidebar.slider(
            "Période",
            min_value=annee_min,
            max_value=annee_max,
            value=(annee_min, annee_max),
            step=1
        )
        self.annee_debut, self.annee_fin = annee_range
        
        # Bouton d'actualisation
        if st.sidebar.button("🔄 Actualiser les données"):
            st.rerun()
        
        # Informations sur la base de données
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📈 Informations")
        info = self.db_manager.get_database_info()
        st.sidebar.write(f"- **Période**: {info.get('periode', 'N/A')}")
        st.sidebar.write(f"- **Départements**: {info.get('total_departements', 0)}")
        st.sidebar.write(f"- **Enregistrements**: {info.get('total_rows', 0):,}")
    
    def render_kpi(self):
        """Affiche les indicateurs clés de performance"""
        st.header("📊 Indicateurs clés")
        
        # Récupérer les KPI
        kpi = self.db_manager.get_kpi_global()
        
        # Créer les colonnes pour les KPI
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="kpi-container">
                <div class="kpi-value">{:,}</div>
                <div class="kpi-label">Emploi total</div>
            </div>
            """.format(kpi.get('total_emploi', 0)), unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="kpi-container">
                <div class="kpi-value">{:,}</div>
                <div class="kpi-label">Entreprises</div>
            </div>
            """.format(kpi.get('total_entreprises', 0)), unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="kpi-container">
                <div class="kpi-value">{:.1f}%</div>
                <div class="kpi-label">Taux de chômage moyen</div>
            </div>
            """.format(kpi.get('taux_chomage_moyen', 0)), unsafe_allow_html=True)
        
        with col4:
            variation = kpi.get('variation_emploi', 0)
            color = "green" if variation >= 0 else "red"
            st.markdown(f"""
            <div class="kpi-container">
                <div class="kpi-value" style="color: {color};">{variation:+.1f}%</div>
                <div class="kpi-label">Variation emploi</div>
            </div>
            """, unsafe_allow_html=True)
    
    def render_evolution_temporelle(self):
        """Affiche le graphique d'évolution temporelle"""
        st.header("📈 Évolution temporelle")
        
        # Récupérer les données d'évolution
        df_evolution = self.db_manager.get_evolution_temporelle(
            departement=self.selected_departement,
            region=self.selected_region
        )
        
        if df_evolution is not None and not df_evolution.empty:
            # Filtrer par période
            df_evolution = df_evolution[
                (df_evolution['annee'] >= self.annee_debut) & 
                (df_evolution['annee'] <= self.annee_fin)
            ]
            
            # Créer le graphique avec sous-graphes
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Emploi total', 'Taux de chômage', 'Nombre d\'entreprises', 'Variation annuelle'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Emploi total
            fig.add_trace(
                go.Scatter(x=df_evolution['annee'], y=df_evolution['emploi_total_moyen'],
                          mode='lines+markers', name='Emploi total'),
                row=1, col=1
            )
            
            # Taux de chômage
            fig.add_trace(
                go.Scatter(x=df_evolution['annee'], y=df_evolution['taux_chomage_moyen'],
                          mode='lines+markers', name='Taux de chômage', line=dict(color='red')),
                row=1, col=2
            )
            
            # Entreprises
            fig.add_trace(
                go.Scatter(x=df_evolution['annee'], y=df_evolution['entreprises_moyennes'],
                          mode='lines+markers', name='Entreprises', line=dict(color='green')),
                row=2, col=1
            )
            
            # Variation
            fig.add_trace(
                go.Bar(x=df_evolution['annee'], y=df_evolution['variation_moyenne'],
                      name='Variation emploi', marker_color='orange'),
                row=2, col=2
            )
            
            fig.update_layout(height=600, showlegend=False, title_text="Évolution des indicateurs")
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.warning("Aucune donnée disponible pour la période sélectionnée")
    
    def render_classement(self):
        """Affiche le classement des départements"""
        st.header("🏆 Classement des départements")
        
        col1, col2 = st.columns(2)
        
        with col1:
            metric = st.selectbox(
                "Métrique de classement",
                ['emploi_total', 'taux_chomage', 'nombre_entreprises', 'variation_emploi_total'],
                format_func=lambda x: {
                    'emploi_total': 'Emploi total',
                    'taux_chomage': 'Taux de chômage',
                    'nombre_entreprises': 'Nombre d\'entreprises',
                    'variation_emploi_total': 'Variation emploi'
                }[x]
            )
        
        with col2:
            annee_classement = st.selectbox(
                "Année",
                self.annees,
                index=len(self.annees) - 1
            )
        
        # Récupérer le classement
        df_classement = self.db_manager.get_classement_departements(
            annee=annee_classement,
            metric=metric
        )
        
        if df_classement is not None and not df_classement.empty:
            # Créer un graphique en barres
            fig = px.bar(
                df_classement.head(15),
                x=metric,
                y='nom_departement',
                orientation='h',
                title=f"Top 15 départements - {metric.replace('_', ' ').title()} ({annee_classement})",
                color=metric,
                color_continuous_scale='viridis'
            )
            
            fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Afficher le tableau
            st.subheader("📋 Tableau détaillé")
            st.dataframe(
                df_classement.head(20),
                use_container_width=True,
                hide_index=True
            )
            
        else:
            st.warning("Aucune donnée disponible pour le classement")
    
    def render_analyse_territoriale(self):
        """Affiche l'analyse territoriale"""
        st.header("🗺️ Analyse territoriale")
        
        if self.selected_region and self.selected_region != "Toutes":
            # Analyse pour une région spécifique
            df_region = self.db_manager.get_stats_region(self.selected_region)
            
            if df_region is not None and not df_region.empty:
                st.subheader(f"Analyse de la région : {self.selected_region}")
                
                # Créer des graphiques pour la région
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_emploi = px.line(
                        df_region, x='annee', y='emploi_total',
                        title=f"Évolution de l'emploi - {self.selected_region}",
                        markers=True
                    )
                    st.plotly_chart(fig_emploi, use_container_width=True)
                
                with col2:
                    fig_chomage = px.line(
                        df_region, x='annee', y='taux_chomage_moyen',
                        title=f"Taux de chômage moyen - {self.selected_region}",
                        markers=True, line=dict(color='red')
                    )
                    st.plotly_chart(fig_chomage, use_container_width=True)
                
                # Statistiques détaillées
                st.subheader("📊 Statistiques détaillées")
                latest_data = df_region.iloc[-1]
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Départements", latest_data['nb_departements'])
                    st.metric("Emploi total", f"{latest_data['emploi_total']:,}")
                
                with col2:
                    st.metric("Taux de chômage", f"{latest_data['taux_chomage_moyen']:.1f}%")
                    st.metric("Entreprises", f"{latest_data['total_entreprises']:,}")
                
                with col3:
                    # Calculer la variation
                    if len(df_region) > 1:
                        variation = ((latest_data['emploi_total'] - df_region.iloc[-2]['emploi_total']) / 
                                   df_region.iloc[-2]['emploi_total'] * 100)
                        st.metric("Variation annuelle", f"{variation:+.1f}%")
            
            else:
                st.warning(f"Aucune donnée disponible pour la région {self.selected_region}")
        
        else:
            st.info("Sélectionnez une région dans la barre latérale pour voir l'analyse territoriale détaillée")
    
    def render_data_table(self):
        """Affiche le tableau de données interactif"""
        st.header("📋 Tableau de données")
        
        # Construire la requête pour les données filtrées
        query = """
            SELECT code_departement, nom_departement, region, annee,
                   emploi_total, emploi_salarie, emploi_non_salarie,
                   taux_chomage, nombre_entreprises, variation_emploi_total
            FROM emploi
            WHERE 1=1
        """
        
        params = {}
        if self.selected_departement:
            query += " AND code_departement = :dept"
            params['dept'] = self.selected_departement
        
        if self.selected_region and self.selected_region != "Toutes":
            query += " AND region = :region"
            params['region'] = self.selected_region
        
        query += " AND annee BETWEEN :annee_debut AND :annee_fin"
        params.update({'annee_debut': self.annee_debut, 'annee_fin': self.annee_fin})
        
        query += " ORDER BY annee DESC, code_departement"
        
        # Limiter le nombre de résultats pour éviter les problèmes de performance
        query += " LIMIT 1000"
        
        df_data = self.db_manager.execute_query(query, params)
        
        if df_data is not None and not df_data.empty:
            st.dataframe(
                df_data,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "code_departement": st.column_config.TextColumn("Code Dept"),
                    "nom_departement": st.column_config.TextColumn("Département"),
                    "region": st.column_config.TextColumn("Région"),
                    "annee": st.column_config.NumberColumn("Année"),
                    "emploi_total": st.column_config.NumberColumn("Emploi total", format="%d"),
                    "emploi_salarie": st.column_config.NumberColumn("Emploi salarié", format="%d"),
                    "emploi_non_salarie": st.column_config.NumberColumn("Emploi non salarié", format="%d"),
                    "taux_chomage": st.column_config.NumberColumn("Taux chômage (%)", format="%.1f"),
                    "nombre_entreprises": st.column_config.NumberColumn("Entreprises", format="%d"),
                    "variation_emploi_total": st.column_config.NumberColumn("Variation (%)", format="%.1f")
                }
            )
            
            # Bouton de téléchargement
            csv = df_data.to_csv(index=False)
            st.download_button(
                label="📥 Télécharger les données (CSV)",
                data=csv,
                file_name="emploi_territorial_data.csv",
                mime="text/csv"
            )
            
        else:
            st.warning("Aucune donnée disponible pour les filtres sélectionnés")
    
    def render(self):
        """Méthode principale pour rendre le dashboard"""
        # Afficher l'en-tête
        self.render_header()
        
        # Afficher la barre latérale
        self.render_sidebar()
        
        # Afficher les KPI
        self.render_kpi()
        
        # Afficher l'évolution temporelle
        self.render_evolution_temporelle()
        
        # Afficher le classement
        self.render_classement()
        
        # Afficher l'analyse territoriale
        self.render_analyse_territoriale()
        
        # Afficher le tableau de données
        self.render_data_table()

def main():
    """Fonction principale"""
    try:
        dashboard = Dashboard()
        dashboard.render()
        
        # Footer
        st.markdown("---")
        st.markdown(
            "<div style='text-align: center; color: #666;'>"
            "📊 Observatoire de l'emploi territorial - Prototype de data analyst"
            "</div>", 
            unsafe_allow_html=True
        )
        
    except Exception as e:
        st.error(f"Une erreur est survenue: {e}")
        st.stop()

if __name__ == "__main__":
    main()
