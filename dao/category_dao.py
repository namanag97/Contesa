#!/usr/bin/env python3
"""
Category Data Access Object
Provides database operations for categories and valid combinations.
"""

import os
import logging
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd

from dao.base_dao import BaseDAO
from utils.error.error_handler import DatabaseError, exception_mapper

# Configure logging
logger = logging.getLogger(__name__)

class CategoryDAO(BaseDAO):
    """Data Access Object for category operations"""
    
    CATEGORIES_TABLE = "categories"
    COMBINATIONS_TABLE = "valid_combinations"
    
    def get_all_categories(self) -> Dict[str, List[str]]:
        """
        Get all categories grouped by level
        
        Returns:
            Dictionary with levels as keys and lists of category names as values
        """
        try:
            query = "SELECT level, category_name FROM {} ORDER BY level, category_name".format(self.CATEGORIES_TABLE)
            results = self.execute_query(query)
            
            # Group by level
            categories = {"L1": [], "L2": [], "L3": []}
            for row in results:
                level = row.get('level', '')
                if level in categories:
                    categories[level].append(row.get('category_name', ''))
            
            return categories
            
        except Exception as e:
            logger.error("Error getting categories: {}".format(str(e)))
            raise DatabaseError("Error getting categories: {}".format(str(e)))
    
    def get_valid_combinations(self) -> List[Dict[str, str]]:
        """
        Get all valid category combinations
        
        Returns:
            List of dictionaries with L1, L2, L3 category combinations
        """
        try:
            query = "SELECT l1_category, l2_category, l3_category FROM {}".format(self.COMBINATIONS_TABLE)
            return self.execute_query(query)
            
        except Exception as e:
            logger.error("Error getting valid combinations: {}".format(str(e)))
            raise DatabaseError("Error getting valid combinations: {}".format(str(e)))
    
    def import_categories_from_csv(self, csv_file: str) -> Tuple[int, int]:
        """
        Import categories and valid combinations from CSV file
        
        Args:
            csv_file: Path to the CSV file
            
        Returns:
            Tuple of (number of categories imported, number of combinations imported)
        """
        try:
            if not os.path.exists(csv_file):
                logger.warning("Categories file not found: {}".format(csv_file))
                return (0, 0)
                
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Identify category columns
            l1_col = None
            l2_col = None
            l3_col = None
            
            # Look for standard column names
            l1_candidates = ['L1_Category', 'Column A: L1 Category', 'Column A: L1 Category (High-Level)', 'L1']
            l2_candidates = ['L2_Category', 'Column B: L2 Category', 'Column B: L2 Category (Mid-Level)', 'L2']
            l3_candidates = ['L3_Category', 'Column C: L3 Category', 'Column C: L3 Category (Granular Detail)', 'L3']
            
            # Find matching columns
            for col in df.columns:
                if col in l1_candidates:
                    l1_col = col
                elif col in l2_candidates:
                    l2_col = col
                elif col in l3_candidates:
                    l3_col = col
            
            # If standard columns not found, try to use first three columns
            if not all([l1_col, l2_col, l3_col]):
                # Try to use first three columns if they exist
                columns = df.columns.tolist()[:3]
                if len(columns) >= 3:
                    logger.info("Using first three columns as L1, L2, L3: {}".format(columns))
                    l1_col, l2_col, l3_col = columns
                else:
                    # Not enough columns available
                    logger.error("Not enough columns in categories file")
                    return (0, 0)
            
            # Extract unique categories
            l1_categories = df[l1_col].dropna().unique().tolist()
            l2_categories = df[l2_col].dropna().unique().tolist()
            l3_categories = df[l3_col].dropna().unique().tolist()
            
            # Clear existing categories
            self.execute_update("DELETE FROM {}".format(self.CATEGORIES_TABLE))
            self.execute_update("DELETE FROM {}".format(self.COMBINATIONS_TABLE))
            
            # Insert L1 categories
            for category in l1_categories:
                self.execute_update(
                    "INSERT INTO {} (level, category_name, description) VALUES (?, ?, ?)".format(self.CATEGORIES_TABLE),
                    ('L1', category, 'Imported from CSV')
                )
            
            # Insert L2 categories
            for category in l2_categories:
                self.execute_update(
                    "INSERT INTO {} (level, category_name, description) VALUES (?, ?, ?)".format(self.CATEGORIES_TABLE),
                    ('L2', category, 'Imported from CSV')
                )
            
            # Insert L3 categories
            for category in l3_categories:
                self.execute_update(
                    "INSERT INTO {} (level, category_name, description) VALUES (?, ?, ?)".format(self.CATEGORIES_TABLE),
                    ('L3', category, 'Imported from CSV')
                )
            
            # Insert valid combinations
            combinations_count = 0
            for _, row in df.iterrows():
                l1 = row.get(l1_col)
                l2 = row.get(l2_col)
                l3 = row.get(l3_col)
                
                # Skip rows with missing values
                if pd.isna(l1) or pd.isna(l2) or pd.isna(l3):
                    continue
                
                self.execute_update(
                    "INSERT INTO {} (l1_category, l2_category, l3_category) VALUES (?, ?, ?)".format(self.COMBINATIONS_TABLE),
                    (l1, l2, l3)
                )
                combinations_count += 1
            
            logger.info("Imported {} categories and {} valid combinations".format(
                len(l1_categories) + len(l2_categories) + len(l3_categories),
                combinations_count
            ))
            
            return (
                len(l1_categories) + len(l2_categories) + len(l3_categories),
                combinations_count
            )
            
        except Exception as e:
            logger.error("Error importing categories from CSV: {}".format(str(e)))
            raise DatabaseError("Error importing categories from CSV: {}".format(str(e)))
    
    def is_valid_combination(self, l1: str, l2: str, l3: str) -> bool:
        """
        Check if a category combination is valid
        
        Args:
            l1: L1 category
            l2: L2 category
            l3: L3 category
            
        Returns:
            True if the combination is valid, False otherwise
        """
        query = """
        SELECT COUNT(*) as count FROM {}
        WHERE l1_category = ? AND l2_category = ? AND l3_category = ?
        """.format(self.COMBINATIONS_TABLE)
        
        results = self.execute_query(query, (l1, l2, l3))
        return results[0]['count'] > 0 if results else False
    
    def get_categories_by_level(self, level: str) -> List[str]:
        """
        Get categories for a specific level
        
        Args:
            level: Level (L1, L2, L3)
            
        Returns:
            List of category names
        """
        query = "SELECT category_name FROM {} WHERE level = ? ORDER BY category_name".format(self.CATEGORIES_TABLE)
        results = self.execute_query(query, (level,))
        return [row.get('category_name', '') for row in results]
    
    def add_category(self, level: str, name: str, description: str = "") -> bool:
        """
        Add a new category
        
        Args:
            level: Level (L1, L2, L3)
            name: Category name
            description: Optional description
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = "INSERT INTO {} (level, category_name, description) VALUES (?, ?, ?)".format(self.CATEGORIES_TABLE)
            self.execute_update(query, (level, name, description))
            return True
        except Exception as e:
            logger.error("Error adding category: {}".format(str(e)))
            return False
    
    def add_valid_combination(self, l1: str, l2: str, l3: str) -> bool:
        """
        Add a new valid category combination
        
        Args:
            l1: L1 category
            l2: L2 category
            l3: L3 category
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = "INSERT INTO {} (l1_category, l2_category, l3_category) VALUES (?, ?, ?)".format(self.COMBINATIONS_TABLE)
            self.execute_update(query, (l1, l2, l3))
            return True
        except Exception as e:
            logger.error("Error adding valid combination: {}".format(str(e)))
            return False 