#!/usr/bin/env python3
"""
Data Access Objects (DAO) package for the Call Center Analysis System.
Provides database interface classes that abstract database operations.
"""

from dao.base_dao import BaseDAO
from dao.transcription_dao import TranscriptionDAO
from dao.analysis_dao import AnalysisResultDAO
from dao.category_dao import CategoryDAO
from dao.stats_dao import StatsDAO
from dao.user_dao import UserDAO
from dao.config_dao import ConfigDAO
from dao.db_connection_pool import get_db_connection, close_all_connections 