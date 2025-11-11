"""
Minecraft Server Statistics Manager
Reorganized and modularized version
"""

import json
import os
import pandas as pd
import configparser
import ftplib
import math
import paramiko
import stat
import nbt
from PIL import Image, ImageDraw, ImageFont, ImageColor
import requests
from io import BytesIO
from typing import Optional, Tuple, Dict, List
from abc import ABC, abstractmethod
from datetime import datetime
import base64

# ============================================================================
# GITHUB UPLOAD
# ============================================================================

class GitHubUploader:
    def __init__(self, token: str, repo_owner: str, repo_name: str, branch: str = "main"):
        self.token = token
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.branch = branch
        self.base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }

    def _get_file_sha(self, filename: str) -> Optional[str]:
        url = f"{self.base_url}/contents/{filename}?ref={self.branch}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()["sha"]
        elif response.status_code == 404:
            # Le fichier n'existe pas encore, c'est ok
            return None
        else:
            print(f"Erreur GitHub _get_file_sha: {response.status_code} - {response.text}")
            return None
#t
    def upload_image(self, image_path: str, filename: str = "leaderboard.png") -> Optional[str]:
        try:
            print(f"📤 Upload de l'image: {image_path}")

            with open(image_path, "rb") as f:
                image_data = base64.b64encode(f.read()).decode()

            sha = self._get_file_sha(filename)
            payload = {
                "message": f"Update leaderboard - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "content": image_data,
                "branch": self.branch
            }
            if sha:
                payload["sha"] = sha

            url = f"{self.base_url}/contents/{filename}"
            response = requests.put(url, headers=self.headers, json=payload)

            if response.status_code in [200, 201]:
                return response.json()["content"]["download_url"]
            else:
                print(f"❌ Erreur upload GitHub: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            print(f"💥 Erreur upload: {e}")
            return None

    def get_latest_commit_url(self, filename: str = "leaderboard.png") -> str:
        return f"https://github.com/{self.repo_owner}/{self.repo_name}/blob/{self.branch}/{filename}?raw=true"
# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

class ConfigManager:
    """Manages configuration loading and validation."""
    
    VALID_MODES = ['manual', 'local', 'ftp', 'sftp']
    
    def __init__(self, config_path: str = 'config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf8')
        self._validate()
    
    def _validate(self):
        mode = self.get('INPUT', 'Mode')
        if mode not in self.VALID_MODES:
            raise ValueError(f"Invalid input mode: {mode}. Must be one of {self.VALID_MODES}")
    
    def get(self, section: str, key: str, fallback: str = "") -> str:
        return self.config.get(section, key, fallback=fallback)
    
    def get_bool(self, section: str, key: str, fallback: bool = False) -> bool:
        return self.config.get(section, key, fallback=str(fallback)).lower() == 'true'
    
    def get_int(self, section: str, key: str, fallback: int = 0) -> int:
        return int(self.config.get(section, key, fallback=str(fallback)))
    
    def get_list(self, section: str, key: str, separator: str = ',') -> List[str]:
        value = self.get(section, key, '')
        return [item.strip() for item in value.split(separator) if item.strip()]


# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================

class ConnectionManager(ABC):
    """Abstract base class for connection managers."""
    
    @abstractmethod
    def download_file(self, remote_path: str, local_path: str):
        pass
    
    @abstractmethod
    def list_directory(self, path: str) -> List[str]:
        pass
    
    @abstractmethod
    def change_directory(self, path: str):
        pass
    
    @abstractmethod
    def get_current_directory(self) -> str:
        pass
    
    @abstractmethod
    def return_to_root(self):
        pass
    
    @abstractmethod
    def close(self):
        pass


class FTPManager(ConnectionManager):
    """FTP connection manager."""
    
    def __init__(self, host: str, username: str, password: str):
        self.ftp = ftplib.FTP(host, username, password)
        self.ftp.encoding = "utf-8"
    
    def download_file(self, remote_path: str, local_path: str):
        with open(local_path, "wb") as file:
            self.ftp.retrbinary(f"RETR {remote_path}", file.write)
    
    def list_directory(self, path: str) -> List[str]:
        return self.ftp.nlst(path)
    
    def change_directory(self, path: str):
        self.ftp.cwd(path)
    
    def get_current_directory(self) -> str:
        return self.ftp.pwd()
    
    def return_to_root(self):
        current = self.get_current_directory()
        depth = len(current.split("/")) - 1
        if depth > 0:
            self.change_directory("../" * depth)
    
    def close(self):
        self.ftp.quit()


class SFTPManager(ConnectionManager):
    """SFTP connection manager."""
    
    def __init__(self, host: str, port: int, username: str, password: str):
        self.transport = paramiko.Transport((host, port))
        self.transport.connect(username=username, password=password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
    
    def download_file(self, remote_path: str, local_path: str):
        from pathlib import Path
    
        print(f"Téléchargement: {remote_path} -> {local_path}")
        
        try:
            # Lire en binaire depuis SFTP
            with self.sftp.open(remote_path, 'rb') as remote_file:
                content = remote_file.read()
            
            # Écrire en binaire localement
            local_dir = Path(local_path).parent
            local_dir.mkdir(parents=True, exist_ok=True)
            
            with open(local_path, 'wb') as local_file:
                local_file.write(content)
                
            print(f"✓ Succès: {len(content)} bytes transférés")
            
        except Exception as e:
            print(f"✗ Erreur: {e}")
            

    def list_directory_debug(self, path: str = "."):
        """Debug helper to list directory contents with types."""
        try:
            print(f"\nContents of directory '{path}':")
            for entry in self.sftp.listdir_attr("/Minecraft/world/stats/"):
                if stat.S_ISDIR(entry.st_mode):
                    file_type = '<DIR>'
                elif stat.S_ISREG(entry.st_mode):
                    file_type = '<FILE>'
                elif stat.S_ISLNK(entry.st_mode):
                    file_type = '<LINK>'
                else:
                    file_type = '<OTHER>'
                print(f"{entry.filename:30} {file_type:8} {entry.st_size:8} bytes")
        except Exception as e:
            print(f"Error listing directory: {e}")
    
    def change_directory(self, path: str):
        try:
            # Vérifier si le path existe et est un dossier
            attrs = self.sftp.stat(path)
        
            if stat.S_ISDIR(attrs.st_mode):
                print(f"📁 {path} est un dossier, changement...")
                self.sftp.chdir(path)
                print(f"✅ Changé vers: {self.sftp.getcwd()}")
            elif stat.S_ISREG(attrs.st_mode):
                raise NotADirectoryError(f"❌ {path} est un fichier, pas un dossier!")
            else:
                raise NotADirectoryError(f"❌ {path} n'est pas un dossier!")
            
        except FileNotFoundError:
            print(f"❌ Le chemin {path} n'existe pas")
            # Montrer le contenu du dossier parent
            parent_dir = os.path.dirname(path)
            if parent_dir:
                print(f"Contenu de {parent_dir}:")
                self.list_directory_debug(parent_dir)
            raise
        except Exception as e:
            print(f"❌ Erreur pour changer de dossier {path}: {e}")
            raise

    def list_directory(self, path: str) -> List[str]:
        """List directory contents, returning only files (not directories)."""
        try:
            # D'abord vérifier que c'est bien un dossier
            attrs = self.sftp.stat(path)
            if not stat.S_ISDIR(attrs.st_mode):
                print(f"ERREUR: {path} n'est pas un dossier!")
                return []
            
            items = self.sftp.listdir_attr(path)
            files = []
            for item in items:
                if stat.S_ISREG(item.st_mode):  # Regular file
                    files.append(item.filename)
            return files
        except Exception as e:
            print(f"Erreur en listant le dossier SFTP {path}: {e}")
            return []
    
    def get_current_directory(self) -> str:
        return self.sftp.getcwd()
    
    def return_to_root(self):
        current = self.get_current_directory()
        depth = len([x for x in current.split("/") if x]) if current != "/" else 0
        if depth > 0:
            self.change_directory("../" * depth)
    
    def close(self):
        self.sftp.close()
        self.transport.close()


def create_connection(mode: str, host: str = "", port: int = 22, 
                     username: str = "", password: str = "") -> Optional[ConnectionManager]:
    """Factory function to create appropriate connection manager."""
    if mode == "ftp":
        return FTPManager(host, username, password)
    elif mode == "sftp":
        return SFTPManager(host, port, username, password)
    return None


# ============================================================================
# DATA LOADING
# ============================================================================

class MinecraftDataLoader:
    """Loads Minecraft server data from various sources."""
    
    def __init__(self, mode: str, connection: Optional[ConnectionManager] = None,
                 ftp_path: str = "", local_path: str = ""):
        self.mode = mode
        self.connection = connection
        self.ftp_path = ftp_path
        self.local_path = local_path
        self.names_df = None
    
    def _get_paths(self) -> Dict[str, str]:
        """Get paths for different data types based on mode."""
        if self.mode in ["ftp", "sftp"]:
            base = self.ftp_path if self.ftp_path else ""
            return {
                'stats': f"Minecraft/world/stats" if base else "world/stats",
                'playerdata': f"{base}/world/playerdata" if base else "world/playerdata",
                'advancements': f"{base}/world/advancements" if base else "world/advancements",
                'usercache': "/Minecraft/usercache.json"
            }
        elif self.mode == "local":
            return {
                'stats': f"{self.local_path}/world/stats/",
                'playerdata': f"{self.local_path}/world/playerdata/",
                'advancements': f"{self.local_path}/world/advancements/",
                'usercache': f"{self.local_path}/usercache.json"
            }
        else:  # manual
            return {
                'stats': 'data/stats',
                'playerdata': 'data/playerdata',
                'advancements': 'data/advancements',
                'usercache': 'data/usercache/usercache.json'
            }
    
    def _load_usercache(self):
        """Load user cache mapping UUIDs to names."""
        paths = self._get_paths()
        
        if self.mode in ["ftp", "sftp"]:
            if self.ftp_path:
                self.connection.change_directory(self.ftp_path)
            self.connection.download_file(paths['usercache'], "data/usercache/usercache.json")
            self.connection.return_to_root()
            cache_path = "data/usercache/usercache.json"
        else:
            cache_path = paths['usercache']
        
        with open(cache_path, 'r') as f:
            self.names_df = pd.DataFrame(json.load(f))
    
    def _get_username(self, uuid: str) -> str:
        """Get username from UUID."""
        temp_name = self.names_df.loc[self.names_df['uuid'] == uuid]['name']
        if temp_name.empty:
            print(f"No username found for UUID {uuid}, using UUID instead.")
            return uuid
        return temp_name.iloc[0]
    
    def _clear_local_data_folders(self):
        """Clear local data folders before downloading new files."""
        if self.mode not in ["ftp", "sftp"]:
            return
        
        for folder in ["data/stats", "data/playerdata", "data/advancements"]:
            for filename in os.listdir(folder):
                if filename == ".gitignore":
                    continue
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    print(f'Failed to remove {file_path}. Reason: {e}')
    
    def _download_and_process_file(self, filename: str, remote_path: str, 
                                   local_folder: str) -> dict:
        """Download a file from remote server and return its JSON content."""
        if self.mode not in ["ftp", "sftp"]:
            return None
        
        if filename.endswith('.'):
            return None
        
        filename = filename.split("/")[-1]
        print(f"Now processing {filename}")
        
        local_file = os.path.join(local_folder, filename)
        self.connection.download_file(filename, local_file)
        
        return local_file
    
    def _process_stats_file(self, filepath: str, uuid: str) -> pd.DataFrame:
        """Process a stats JSON file into a DataFrame."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        temp_df = pd.json_normalize(data, meta_prefix=True)
        username = self._get_username(uuid)
        temp_df = temp_df.transpose().iloc[1:].rename({0: username}, axis=1)
        temp_df.index = temp_df.index.str.split('.', expand=True)
        
        # Handle stats with dots in their names
        if len(temp_df.index.levshape) > 3:
            temp_df.index = temp_df.index.droplevel(3)
            temp_df = temp_df.groupby(level=[0, 1, 2]).sum()
        
        return temp_df
    
    def _process_playerdata_file(self, filepath: str, uuid: str) -> Dict[str, any]:
        """Process a playerdata NBT file and extract relevant info."""
        username = self._get_username(uuid)
        nbtfile = nbt.nbt.NBTFile(filepath, 'r')
        
        money = math.floor(nbtfile['cardinal_components']['numismatic-overhaul:currency']['Value'].value / 10000)
        waystones = len(nbtfile['BalmData']['WaystonesData']['Waystones'])
        
        return {
            'username': username,
            'money': money,
            'waystones': waystones
        }
    
    def _process_advancements_file(self, filepath: str, uuid: str) -> pd.DataFrame:
        """Process an advancements JSON file into a DataFrame."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        temp_df = pd.json_normalize(data, meta_prefix=True)
        username = self._get_username(uuid)
        temp_df = temp_df.transpose().iloc[1:].rename({0: username}, axis=1)
        temp_df.index = temp_df.index.str.split('.', expand=True)
        
        # Remove recipe advancements
        temp_df = temp_df[~temp_df.index.get_level_values(0).str.split(":", n=1).str[1].str.startswith("recipes")]
        
        return temp_df
    
    def _load_stats_remote(self, paths: Dict[str, str]) -> pd.DataFrame:
        """Charge les stats SANS utiliser change_directory."""
        df = pd.DataFrame()
        
        try:
            # Lister les fichiers DIRECTEMENT du chemin absolu
            stats_path = "/Minecraft/world/stats"
            print(f"📁 Listing direct de: {stats_path}")
            
            # Utiliser listdir_attr directement
            items = self.connection.sftp.listdir_attr(stats_path)
            
            for item in items:
                if stat.S_ISREG(item.st_mode) and item.filename.endswith('.json'):
                    filename = item.filename
                    print(f"🔄 Traitement de {filename}")
                    
                    # Chemin complet pour le téléchargement
                    remote_file = f"{stats_path}/{filename}"
                    local_file = os.path.join('data/stats', filename)
                    
                    # Télécharger directement
                    self.connection.download_file(remote_file, local_file)
                    
                    uuid = filename[:-5]
                    temp_df = self._process_stats_file(local_file, uuid)
                    df = temp_df if df.empty else df.join(temp_df, how="outer")
                    
        except Exception as e:
            print(f"💥 Erreur: {e}")
            import traceback
            traceback.print_exc()
        
        return df
    
    def _load_stats_local(self, paths: Dict[str, str]) -> pd.DataFrame:
        """Load stats from local files."""
        df = pd.DataFrame()
        
        for filename in os.listdir(paths['stats']):
            if filename == ".gitignore":
                continue
            
            print(f"Now processing {filename}")
            filepath = os.path.join(paths['stats'], filename)
            uuid = filename[:-5]
            temp_df = self._process_stats_file(filepath, uuid)
            
            df = temp_df if df.empty else df.join(temp_df, how="outer")
        
        return df
    
    def _load_playerdata_remote(self, paths: Dict[str, str]) -> Tuple[Dict, Dict]:
        """Load playerdata from remote server - VERSION DIRECTE."""
        money = {}
        waystones = {}
        
        try:
            # Utiliser le chemin absolu directement
            playerdata_path = "/Minecraft/world/playerdata"
            print(f"📁 Listing direct de: {playerdata_path}")
            
            # Lister les fichiers directement
            items = self.connection.sftp.listdir_attr(playerdata_path)
            
            for item in items:
                filename = item.filename
                # Filtrer les fichiers .dat
                if (stat.S_ISREG(item.st_mode) and 
                    filename.endswith('.dat') and 
                    not filename.endswith('_old') and 
                    filename != "player_roles"):
                    
                    print(f"🔄 Traitement de {filename}")
                    
                    # Télécharger directement avec le chemin complet
                    remote_file = f"{playerdata_path}/{filename}"
                    local_file = os.path.join('data/playerdata', filename)
                    
                    self.connection.download_file(remote_file, local_file)
                    
                    uuid = filename[:-4]
                    player_data = self._process_playerdata_file(local_file, uuid)
                    money[player_data['username']] = player_data['money']
                    waystones[player_data['username']] = player_data['waystones']
                    
        except Exception as e:
            print(f"💥 Erreur playerdata: {e}")
            import traceback
            traceback.print_exc()
        
        return money, waystones
    
    def _load_playerdata_local(self, paths: Dict[str, str]) -> Tuple[Dict, Dict]:
        """Load playerdata from local files."""
        money = {}
        waystones = {}
        
        for filename in os.listdir(paths['playerdata']):
            if filename in ['.', '..', '.gitignore', 'player_roles'] or filename.endswith('_old'):
                continue
            
            print(f"Now processing {filename}")
            filepath = os.path.join(paths['playerdata'], filename)
            uuid = filename[:-4]
            player_data = self._process_playerdata_file(filepath, uuid)
            money[player_data['username']] = player_data['money']
            waystones[player_data['username']] = player_data['waystones']
        
        return money, waystones
    
    def _load_advancements_remote(self, paths: Dict[str, str]) -> pd.DataFrame:
        """Load advancements from remote server - VERSION DIRECTE."""
        df = pd.DataFrame()
        
        try:
            advancements_path = "/Minecraft/world/advancements"
            print(f"📁 Listing direct de: {advancements_path}")
            
            items = self.connection.sftp.listdir_attr(advancements_path)
            
            for item in items:
                filename = item.filename
                if stat.S_ISREG(item.st_mode) and filename.endswith('.json'):
                    print(f"🔄 Traitement de {filename}")
                    
                    remote_file = f"{advancements_path}/{filename}"
                    local_file = os.path.join('data/advancements', filename)
                    
                    self.connection.download_file(remote_file, local_file)
                    
                    uuid = filename[:-5]
                    temp_df = self._process_advancements_file(local_file, uuid)
                    
                    df = temp_df if df.empty else df.join(temp_df, how="outer")
                    
        except Exception as e:
            print(f"💥 Erreur advancements: {e}")
            import traceback
            traceback.print_exc()
        
        return df
    
    def _load_advancements_local(self, paths: Dict[str, str]) -> pd.DataFrame:
        """Load advancements from local files."""
        df = pd.DataFrame()
        
        for filename in os.listdir(paths['advancements']):
            if filename == ".gitignore":
                continue
            
            print(f"Now processing {filename}")
            filepath = os.path.join(paths['advancements'], filename)
            uuid = filename[:-5]
            temp_df = self._process_advancements_file(filepath, uuid)
            
            df = temp_df if df.empty else df.join(temp_df, how="outer")
        
        return df
    
    def load_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all Minecraft data (stats, money, waystones, advancements).
        
        Returns:
            Tuple of (stats_df, money_df, waystones_df, advancements_df)
        """
        self._load_usercache()
        self._clear_local_data_folders()
        paths = self._get_paths()
        
        # Load stats
        if self.mode in ["ftp", "sftp"]:
            stats_df = self._load_stats_remote(paths)
        else:
            stats_df = self._load_stats_local(paths)
        
        # Load playerdata
        if self.mode in ["ftp", "sftp"]:
            money, waystones = self._load_playerdata_remote(paths)
        else:
            money, waystones = self._load_playerdata_local(paths)
        
        money_df = pd.DataFrame(money, index=["money"]).transpose()
        waystones_df = pd.DataFrame(waystones, index=["waystones"]).transpose()
        
        # Load advancements
        if self.mode in ["ftp", "sftp"]:
            advancements_df = self._load_advancements_remote(paths)
        else:
            advancements_df = self._load_advancements_local(paths)
        
        # Fill missing values
        stats_df = stats_df.fillna(0)
        advancements_df = advancements_df.fillna(0)
        
        return stats_df, money_df, waystones_df, advancements_df


# ============================================================================
# LEADERBOARD GENERATION
# ============================================================================

class LeaderboardGenerator:
    """Generates leaderboards from Minecraft statistics."""
    
    @staticmethod
    def get_vanilla_leaderboard(df: pd.DataFrame, category: str, 
                               subcategory: str, verbose: bool = True) -> pd.DataFrame:
        """Generate a vanilla statistics leaderboard.
        
        Args:
            df: Stats DataFrame
            category: Stat category (e.g., 'minecraft:custom')
            subcategory: Stat subcategory or 'total'
            verbose: Whether to print the leaderboard
            
        Returns:
            Leaderboard DataFrame
        """
        if subcategory == "total":
            row = df.loc['stats'].loc[category].sum().sort_values().iloc[::-1]
        else:
            row = df.loc['stats'].loc[category].loc[subcategory].sort_values().iloc[::-1]
        
        result_df = pd.DataFrame(row).rename(columns={subcategory: 0})
        
        # Format playtime
        if category == "minecraft:custom" and subcategory == "minecraft:play_time":
            result_df[0] = result_df[0].apply(
                lambda x: f"{(int(x) // (20*60*60))}h {((int(x)) // (20*60))%60}min"
            )
        
        if verbose:
            print(f"Leaderboard of {category} {subcategory}:")
            print(result_df)
        
        return result_df
    
    @staticmethod
    def get_advancements_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
        """Generate advancements leaderboard.
        
        Args:
            df: Advancements DataFrame
            
        Returns:
            Leaderboard DataFrame with advancement counts
        """
        count_df = pd.DataFrame((df == True).sum().sort_values())
        count_df['index'] = range(len(count_df), 0, -1)
        count_df = count_df.iloc[::-1]
        return count_df


# ============================================================================
# IMAGE GENERATION
# ============================================================================

class LeaderboardGenerator:
    """Generates leaderboards from Minecraft statistics."""
    
    @staticmethod
    def get_vanilla_leaderboard(df: pd.DataFrame, category: str, 
                               subcategory: str, verbose: bool = True) -> pd.DataFrame:
        """Generate a vanilla statistics leaderboard.
        
        Args:
            df: Stats DataFrame
            category: Stat category (e.g., 'minecraft:custom')
            subcategory: Stat subcategory or 'total'
            verbose: Whether to print the leaderboard
            
        Returns:
            Leaderboard DataFrame
        """
        if subcategory == "total":
            row = df.loc['stats'].loc[category].sum().sort_values().iloc[::-1]
        else:
            row = df.loc['stats'].loc[category].loc[subcategory].sort_values().iloc[::-1]
        
        result_df = pd.DataFrame(row).rename(columns={subcategory: 0})
        
        # Format playtime
        if category == "minecraft:custom" and subcategory == "minecraft:play_time":
            result_df[0] = result_df[0].apply(
                lambda x: f"{(int(x) // (20*60*60))}h {((int(x)) // (20*60))%60}min"
            )
        
        if verbose:
            print(f"Leaderboard of {category} {subcategory}:")
            print(result_df)
        
        return result_df
    
    @staticmethod
    def get_advancements_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
        """Generate advancements leaderboard.
        
        Args:
            df: Advancements DataFrame
            
        Returns:
            Leaderboard DataFrame with advancement counts
        """
        count_df = pd.DataFrame((df == True).sum().sort_values())
        count_df['index'] = range(len(count_df), 0, -1)
        count_df = count_df.iloc[::-1]
        return count_df


# ============================================================================
# IMAGE GENERATION
# ============================================================================

class LeaderboardImageGenerator:
    """Generates leaderboard images with upload capability."""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.usernames_df = self._load_custom_usernames()
        self.github_uploader = None
        
        # Initialize GitHub uploader if enabled
        if config.get_bool('GITHUB', 'Enable', False):
            token = config.get('GITHUB', 'Token', '')
            repo_owner = config.get('GITHUB', 'RepoOwner', '')
            repo_name = config.get('GITHUB', 'RepoName', '')
            
            if token and repo_owner and repo_name:
                self.github_uploader = GitHubUploader(
                    token=token,
                    repo_owner=repo_owner,
                    repo_name=repo_name,
                    branch=config.get('GITHUB', 'Branch', 'main')
                )
            else:
                print("⚠️  GitHub enabled but missing Token, RepoOwner or RepoName")
    
    def _load_custom_usernames(self) -> pd.DataFrame:
        """Load custom usernames mapping."""
        try:
            return pd.read_csv('staticdata/leaderboard_usernames.csv')
        except FileNotFoundError:
            return pd.DataFrame(columns=['minecraft', 'real'])
    
    def _get_display_username(self, minecraft_name: str) -> str:
        """Get display username (custom or Minecraft name)."""
        username = self.usernames_df.loc[
            self.usernames_df['minecraft'] == minecraft_name
        ]
        return username['real'].iloc[0] if not username.empty else minecraft_name
    
    def _download_avatar(self, minecraft_name: str) -> Image.Image:
        """Download player avatar from mc-heads.net."""
        response = requests.get(f"https://mc-heads.net/avatar/{minecraft_name}")
        avatar = Image.open(BytesIO(response.content))
        return avatar.resize((64, 64), Image.Resampling.LANCZOS)
    
    def _create_gradient_background(self, width: int, height: int, 
                                   color1: tuple, color2: tuple) -> Image.Image:
        """Create a gradient background."""
        base = Image.new('RGBA', (width, height), color1)
        top = Image.new('RGBA', (width, height), color2)
        mask = Image.new('L', (width, height))
        mask_data = []
        for y in range(height):
            mask_data.extend([int(255 * (y / height))] * width)
        mask.putdata(mask_data)
        base.paste(top, (0, 0), mask)
        return base
    
    def _draw_minecraft_style_border(self, draw: ImageDraw.Draw, 
                                     x: int, y: int, w: int, h: int,
                                     border_color: tuple, bg_color: tuple):
        """Draw a Minecraft-style bordered panel."""
        # Convert string colors to RGB tuples if needed
        if isinstance(border_color, str):
            border_color = ImageColor.getrgb(border_color)
        if isinstance(bg_color, str):
            bg_color = ImageColor.getrgb(bg_color)
        
        # Background
        draw.rectangle([x+4, y+4, x+w-4, y+h-4], fill=bg_color)
        
        # Borders (pixel-perfect Minecraft style)
        # Top and bottom thick borders
        draw.rectangle([x, y, x+w, y+3], fill=border_color)
        draw.rectangle([x, y+h-3, x+w, y+h], fill=border_color)
        # Left and right borders
        draw.rectangle([x, y, x+3, y+h], fill=border_color)
        draw.rectangle([x+w-3, y, x+w, y+h], fill=border_color)
        
        # Highlight (lighter border on top-left for 3D effect)
        highlight = tuple(min(int(c) + 40, 255) for c in border_color[:3])
        draw.line([x+3, y+3, x+w-3, y+3], fill=highlight, width=1)
        draw.line([x+3, y+3, x+3, y+h-3], fill=highlight, width=1)
        
        # Shadow (darker border on bottom-right)
        shadow = tuple(max(int(c) - 40, 0) for c in border_color[:3])
        draw.line([x+w-4, y+4, x+w-4, y+h-3], fill=shadow, width=1)
        draw.line([x+4, y+h-4, x+w-4, y+h-4], fill=shadow, width=1)
    
    def _draw_rank_badge(self, draw: ImageDraw.Draw, x: int, y: int, 
                        rank: int, size: int = 40):
        """Draw a rank badge with special colors for top 3."""
        colors = {
            1: ('#FFD700', '#FFA500'),  # Gold
            2: ('#C0C0C0', '#808080'),  # Silver
            3: ('#CD7F32', '#8B4513'),  # Bronze
        }
        
        if rank in colors:
            main_color, shadow_color = colors[rank]
        else:
            main_color, shadow_color = ('#555555', '#333333')
        
        # Draw badge background
        self._draw_minecraft_style_border(
            draw, x, y, size, size,
            border_color=shadow_color,
            bg_color=main_color
        )
        
        # Draw rank number
        font = ImageFont.truetype("fonts/Minecraft-Seven_v2.ttf", 20)
        text = str(rank)
        bbox = draw.textbbox((0, 0), text, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        
        # Shadow
        draw.text((x + (size - text_w) // 2 + 2, y + (size - text_h) // 2 + 2),
                 text, fill='#000000', font=font)
        # Main text
        draw.text((x + (size - text_w) // 2, y + (size - text_h) // 2),
                 text, fill='#FFFFFF', font=font)

    def _get_font(self, filename, size):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        font_path = os.path.join(base_dir, "fonts", filename)
        print(f"Loading font: {font_path}")
        if not os.path.exists(font_path):
            print("⚠️ Font file not found, using default font.")
            return ImageFont.load_default()
        return ImageFont.truetype(font_path, size)
    
    def _draw_empty_slot(self, draw: ImageDraw.Draw, entry_x: int, entry_y: int,
                        entry_height: int, panel_w: int, rank: int, slot_index: int,
                        font_name, font_score):
        """Draw an empty player slot."""
        # Entry background (same alternating pattern)
        if slot_index % 2 == 0:
            entry_bg = (40, 40, 40, 150)
        else:
            entry_bg = (50, 50, 50, 150)
        
        draw.rectangle(
            [entry_x, entry_y, entry_x + panel_w - 30, entry_y + entry_height - 5],
            fill=entry_bg
        )
        
        # Rank badge (grayed out)
        self._draw_rank_badge(draw, entry_x + 10, entry_y + 10, rank, size=50)
        
        # Empty avatar slot
        draw.rectangle([entry_x + 70, entry_y + 3, entry_x + 138, entry_y + 71],
                     fill=(30, 30, 30), outline=(60, 60, 60), width=2)
        
        # "Empty slot" text
        name_x = entry_x + 150
        name_y = entry_y + 12
        empty_text = "---"
        
        # Name shadow
        draw.text((name_x + 1, name_y + 1), empty_text, fill='#000000', font=font_name)
        # Name text (grayed out)
        draw.text((name_x, name_y), empty_text, fill='#666666', font=font_name)
        
        # Score (grayed out)
        score_y = entry_y + 38
        draw.text((name_x + 1, score_y + 1), "Score: ---", 
                 fill='#000000', font=font_score)
        draw.text((name_x, score_y), "Score: ---",
                 fill='#444444', font=font_score)
    
    def _draw_leaderboard_section_modern(self, img: Image.Image, draw: ImageDraw.Draw,
                                        df: pd.DataFrame, x_offset: int, y_offset: int,
                                        title: str, base_width: int, base_height: int,
                                        start_rank: int, end_rank: int):
        """Draw a modern Minecraft-style leaderboard section with ranks from start_rank to end_rank."""
        padding = 20
        panel_x = x_offset + padding
        panel_y = y_offset + padding
        panel_w = base_width - (padding * 2)
        panel_h = base_height - (padding * 2)
        
        # Main panel with Minecraft border
        self._draw_minecraft_style_border(
            draw, panel_x, panel_y, panel_w, panel_h,
            border_color=(139, 90, 43),  # Dark wood color
            bg_color=(50, 50, 50, 230)   # Semi-transparent dark background
        )
        
        # Title bar
        title_h = 60
        self._draw_minecraft_style_border(
            draw, panel_x + 10, panel_y + 10, panel_w - 20, title_h,
            border_color=(218, 165, 32),  # Goldenrod
            bg_color=(34, 34, 34, 250)
        )
        
        # Draw title
        font_title = self._get_font("Minecraft-Seven_v2.ttf", 28)
        font_name = self._get_font("minecraft.ttf", 20)
        font_score = self._get_font("Minecraft-Seven_v2.ttf", 18)
        bbox = draw.textbbox((0, 0), title, font=font_title)
        title_w = bbox[2] - bbox[0]
        title_x = panel_x + (panel_w - title_w) // 2
        title_y = panel_y + 25
        
        # Title shadow
        draw.text((title_x + 2, title_y + 2), title, fill='#000000', font=font_title)
        # Title text with gradient effect
        draw.text((title_x, title_y), title, fill='#FFD700', font=font_title)
        
        # Draw player entries (draw slots from start_rank to end_rank)
        entry_start_y = panel_y + 80
        entry_height = 70
        
        actual_players = len(df)
        nb_slots = end_rank - start_rank + 1
        
        for slot_index in range(nb_slots):
            rank = start_rank + slot_index
            entry_y = entry_start_y + slot_index * entry_height
            entry_x = panel_x + 15
            
            if rank <= actual_players:
                # Draw actual player
                player = df.iloc[rank - 1]
                
                # Entry background (alternating colors)
                if slot_index % 2 == 0:
                    entry_bg = (40, 40, 40, 200)
                else:
                    entry_bg = (50, 50, 50, 200)
                
                draw.rectangle(
                    [entry_x, entry_y, entry_x + panel_w - 30, entry_y + entry_height - 5],
                    fill=entry_bg
                )
                
                # Rank badge
                self._draw_rank_badge(draw, entry_x + 10, entry_y + 10, rank, size=50)
                
                # Player avatar
                try:
                    avatar = self._download_avatar(player.name)
                    # Add border to avatar
                    avatar_with_border = Image.new('RGBA', (68, 68), (139, 90, 43))
                    avatar_with_border.paste(avatar, (2, 2))
                    img.paste(avatar_with_border, (entry_x + 70, entry_y + 3), avatar_with_border)
                except:
                    # Fallback if avatar download fails
                    draw.rectangle([entry_x + 70, entry_y + 3, entry_x + 138, entry_y + 71],
                                 fill=(100, 100, 100))
                
                # Player name
                username = self._get_display_username(player.name)
                name_x = entry_x + 150
                name_y = entry_y + 12
                
                # Name shadow
                draw.text((name_x + 1, name_y + 1), username, fill='#000000', font=font_name)
                # Name text
                draw.text((name_x, name_y), username, fill='#FFFFFF', font=font_name)
                
                # Score
                score = player.iloc[0]
                if not isinstance(score, str):
                    score = f"{int(score):,}"  # Format with thousands separator
                
                score_y = entry_y + 38
                # Score shadow
                draw.text((name_x + 1, score_y + 1), f"Score: {score}", 
                         fill='#000000', font=font_score)
                # Score text
                draw.text((name_x, score_y), f"Score: {score}",
                         fill='#55FF55', font=font_score)
            else:
                # Draw empty slot
                self._draw_empty_slot(draw, entry_x, entry_y, entry_height, 
                                    panel_w, rank, slot_index, font_name, font_score)
        
        return img
    
    def generate_top_image(self, leaderboards: List[pd.DataFrame], 
                          titles: List[str]) -> Optional[str]:
        """Generate modern Minecraft-style leaderboards image with equal columns.
        
        Returns:
            GitHub raw URL if upload successful, None otherwise
        """
        # Get configuration
        image_path = self.config.get("TOPIMAGE", "ImagePath")
        
        # Configuration du nombre total de joueurs et de colonnes
        total_players = 30  # Nombre total de joueurs à afficher
        num_columns = 3     # Nombre de colonnes
        
        # Calcul du nombre de joueurs par colonne
        players_per_column = total_players // num_columns
        remaining_players = total_players % num_columns
        
        # Dimensions
        entry_height = 70
        base_width = 650
        base_height = 200 + (players_per_column * entry_height) - 70
        
        # 3 colonnes côte à côte
        width = base_width * num_columns
        height = base_height
        
        # Create gradient background (dark blue to black, Minecraft style)
        img = self._create_gradient_background(
            width, height,
            (15, 15, 35, 0),   # Dark blue-ish
            (15, 15, 35, 0)       # Almost black
        )
        
        # Add subtle texture pattern
        draw = ImageDraw.Draw(img, 'RGBA')
        for i in range(0, width, 4):
            for j in range(0, height, 4):
                if (i + j) % 8 == 0:
                    draw.point((i, j), fill=(255, 255, 255, 10))
        
        # Définir les plages de rangs pour chaque colonne
        rank_ranges = []
        start_rank = 1
        
        for col in range(num_columns):
            # Répartir les joueurs restants sur les premières colonnes
            end_rank = start_rank + players_per_column - 1
            if col < remaining_players:
                end_rank += 1
            
            section_title = f"RANKS {start_rank}-{end_rank}"
            rank_ranges.append((start_rank, end_rank, section_title))
            
            start_rank = end_rank + 1
        
        # Dessiner les sections de classement
        for i, (start_rank, end_rank, section_title) in enumerate(rank_ranges):
            x_offset = i * base_width
            y_offset = 0
            
            # Utiliser les données du premier leaderboard et le premier titre
            df = leaderboards[0] if leaderboards else pd.DataFrame()
            main_title = titles[0] if titles else "Leaderboard"
            full_title = f"{main_title} - {section_title}"
            
            img = self._draw_leaderboard_section_modern(
                img, ImageDraw.Draw(img, 'RGBA'), df,
                x_offset, y_offset, full_title,
                base_width, base_height,
                start_rank, end_rank
            )
        
        # Save image
        img.save(image_path)
        print(f"✅ Modern Minecraft-style leaderboard saved to {image_path}")
        
        # Upload to GitHub if enabled
        github_url = None
        if self.github_uploader:
            github_url = self.github_uploader.upload_image(image_path, "leaderboard.png")
            
            # If direct upload fails, use the commit URL
            if not github_url:
                github_url = self.github_uploader.get_latest_commit_url()
                print(f"📝 Utilisation de l'URL de commit: {github_url}")
            
            # Save URL to file
            if github_url:
                self._save_image_url(github_url)
        
        return github_url
    
    def _save_image_url(self, url: str):
        """Save the image URL to a file for easy access."""
        try:
            with open('latest_leaderboard_url.txt', 'w') as f:
                f.write(url)
            print(f"📝 URL sauvegardée dans latest_leaderboard_url.txt")
            
            # Also create a direct link file for easy sharing
            with open('LINK.md', 'w') as f:
                f.write(f"# Latest Leaderboard\n\n")
                f.write(f"![Minecraft Leaderboard]({url})\n\n")
                f.write(f"**URL directe:** {url}\n")
                f.write(f"*Généré le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
                
        except Exception as e:
            print(f"⚠️  Impossible de sauvegarder l'URL: {e}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    # Load configuration
    print("Loading configuration...")
    config = ConfigManager()
    
    # Read credentials
    username = open("username.txt", "r").read().strip()
    password = open("password.txt", "r").read().strip()
    
    # Create connection if needed
    connection = None
    mode = config.get('INPUT', 'Mode')
    
    if mode in ["ftp", "sftp"]:
        print(f"Connecting to {mode.upper()} server...")
        host = config.get('INPUT', 'Host')
        port = config.get_int('INPUT', 'Port', 22)
        connection = create_connection(mode, host, port, username, password)
    
    try:
        # Load data
        print("\nLOADING VANILLA DATA")
        loader = MinecraftDataLoader(
            mode=mode,
            connection=connection,
            ftp_path=config.get('INPUT', 'FTPPath'),
            local_path=config.get('INPUT', 'LocalPath')
        )
        
        vanilla_df, money_df, waystones_df, advancements_df = loader.load_all_data()
        
        # Save CSVs if enabled
        if config.get_bool('VANILLALEADERBOARD', 'CreateCSV'):
            csv_path = config.get('VANILLALEADERBOARD', 'CSVPath')
            vanilla_df.to_csv(csv_path)
            print(f"Stats saved to {csv_path}")
        
        if config.get_bool('VANILLALEADERBOARD', 'CreateCSVMoney'):
            csv_path_money = config.get('VANILLALEADERBOARD', 'CSVPathMoney')
            money_df.to_csv(csv_path_money)
            print(f"Money data saved to {csv_path_money}")
        
        # Generate test leaderboard if enabled
        if config.get_bool('VANILLALEADERBOARD', 'Enable'):
            print("\nGenerating test leaderboard...")
            category = config.get('VANILLALEADERBOARD', 'Category')
            subcategory = config.get('VANILLALEADERBOARD', 'Subcategory')
            LeaderboardGenerator.get_vanilla_leaderboard(vanilla_df, category, subcategory)
        
        # Generate top image if enabled
        if config.get_bool('TOPIMAGE', 'Enable'):
            print("\nGenerating top leaderboards image...")
            leaderboards_to_show = []
            
            leaderboard_specs = config.get_list('TOPIMAGE', 'Leaderboards')
            titles = config.get_list('TOPIMAGE', 'Titles')
            
            for spec in leaderboard_specs:
                parts = spec.split('/')
                print(f"Preparing leaderboard: {spec}")
                
                if parts[0] == "vanilla":
                    if parts[1] == "advancements":
                        lb = LeaderboardGenerator.get_advancements_leaderboard(advancements_df)
                    else:
                        lb = LeaderboardGenerator.get_vanilla_leaderboard(
                            vanilla_df, parts[1], parts[2], verbose=False
                        )
                    leaderboards_to_show.append(lb)
            
            # Generate image and upload
            image_gen = LeaderboardImageGenerator(config)
            github_url = image_gen.generate_top_image(leaderboards_to_show, titles)
            
            if github_url:
                print(f"\n🌐 Leaderboard disponible sur GitHub: {github_url}")
                print("📋 Lien direct pour partager:")
                print(f"   {github_url}")
                
                # Create a nice preview
                print("\n🖼️  Preview:")
                print(f"   ![Leaderboard]({github_url})")
            else:
                print("❌ Aucun lien généré (GitHub désactivé ou erreur d'upload)")
        
    finally:
        # Close connection
        if connection:
            print("\nClosing connection...")
            connection.close()
    
    print("\nDone!")


if __name__ == "__main__":
    main()