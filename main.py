"""
Minecraft Server Statistics Manager
— version nettoyée pour GitHub Actions + upload images/top3.png —
"""

import json, os, math, base64, stat
import pandas as pd
import configparser
import ftplib
import paramiko
import nbt
from PIL import Image, ImageDraw, ImageFont, ImageColor
import requests
from io import BytesIO
from typing import Optional, Tuple, Dict, List
from abc import ABC, abstractmethod
from datetime import datetime

# ============================================================================
# GITHUB UPLOAD
# ============================================================================

class GitHubUploader:
    """
    Uploade un fichier dans le dépôt via l'API GitHub.
    - utilise en priorité l’ENV `GITHUB_TOKEN` (GitHub Actions)
    - fallback sur `GH_TOKEN` ou le token passé en paramètre/config.ini
    """
    def __init__(self, token: str, repo_owner: str, repo_name: str, branch: str = "main"):
        env_token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
        self.token = env_token or token
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.branch = branch
        self.base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
        if not self.token:
            print("⚠️  Aucun token GitHub fourni (GITHUB_TOKEN/GH_TOKEN/Token). L'upload échouera.")
        self.headers = {
            "Authorization": f"token {self.token}" if self.token else "",
            "Accept": "application/vnd.github.v3+json"
        }

    def _get_file_sha(self, filename: str) -> Optional[str]:
        url = f"{self.base_url}/contents/{filename}?ref={self.branch}"
        r = requests.get(url, headers=self.headers)
        if r.status_code == 200:
            return r.json()["sha"]
        if r.status_code == 404:
            return None
        print(f"Erreur GitHub _get_file_sha: {r.status_code} - {r.text}")
        return None

    def upload_image(self, image_path: str, filename: str = "images/top3.png") -> Optional[str]:
        """
        Commit l’image au chemin `filename` (ex: images/top3.png).
        Retourne l’URL de téléchargement direct (blob?raw=true).
        """
        try:
            print(f"📤 Upload de l'image: {image_path} -> {filename}")
            with open(image_path, "rb") as f:
                content_b64 = base64.b64encode(f.read()).decode()

            sha = self._get_file_sha(filename)
            payload = {
                "message": f"Update leaderboard - {datetime.now():%Y-%m-%d %H:%M:%S}",
                "content": content_b64,
                "branch": self.branch
            }
            if sha:
                payload["sha"] = sha

            url = f"{self.base_url}/contents/{filename}"
            r = requests.put(url, headers=self.headers, json=payload)
            if r.status_code in (200, 201):
                # URL 'download_url' pointe vers raw.githubusercontent.com (ok),
                # mais on renvoie aussi une URL "blob?raw=true" stable.
                return self.get_latest_commit_url(filename)
            print(f"❌ Erreur upload GitHub: {r.status_code} - {r.text}")
            return None
        except Exception as e:
            print(f"💥 Erreur upload: {e}")
            return None

    def get_latest_commit_url(self, filename: str = "images/top3.png") -> str:
        return f"https://github.com/{self.repo_owner}/{self.repo_name}/blob/{self.branch}/{filename}?raw=true"


# ============================================================================
# CONFIGURATION
# ============================================================================

class ConfigManager:
    VALID_MODES = ['manual', 'local', 'ftp', 'sftp']
    def __init__(self, path: str = 'config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(path, encoding='utf8')
        mode = self.get('INPUT', 'Mode')
        if mode not in self.VALID_MODES:
            raise ValueError(f"Invalid input mode: {mode}. Must be one of {self.VALID_MODES}")

    def get(self, sec, key, fallback=""):
        return self.config.get(sec, key, fallback=fallback)

    def get_bool(self, sec, key, fallback=False):
        return self.config.get(sec, key, fallback=str(fallback)).lower() == "true"

    def get_int(self, sec, key, fallback=0):
        return int(self.config.get(sec, key, fallback=str(fallback)))

    def get_list(self, sec, key, sep=','):
        v = self.get(sec, key, '')
        return [x.strip() for x in v.split(sep) if x.strip()]


# ============================================================================
# CONNEXIONS (FTP/SFTP)
# ============================================================================

class ConnectionManager(ABC):
    @abstractmethod
    def download_file(self, remote_path: str, local_path: str): ...
    @abstractmethod
    def list_directory(self, path: str) -> List[str]: ...
    @abstractmethod
    def change_directory(self, path: str): ...
    @abstractmethod
    def get_current_directory(self) -> str: ...
    @abstractmethod
    def return_to_root(self): ...
    @abstractmethod
    def close(self): ...

class FTPManager(ConnectionManager):
    def __init__(self, host, username, password):
        self.ftp = ftplib.FTP(host, username, password)
        self.ftp.encoding = "utf-8"
    def download_file(self, remote_path, local_path):
        with open(local_path, "wb") as f: self.ftp.retrbinary(f"RETR {remote_path}", f.write)
    def list_directory(self, path): return self.ftp.nlst(path)
    def change_directory(self, path): self.ftp.cwd(path)
    def get_current_directory(self): return self.ftp.pwd()
    def return_to_root(self):
        cur = self.get_current_directory()
        depth = len(cur.split("/")) - 1
        if depth > 0: self.change_directory("../" * depth)
    def close(self): self.ftp.quit()

class SFTPManager(ConnectionManager):
    def __init__(self, host, port, username, password):
        self.transport = paramiko.Transport((host, port))
        self.transport.connect(username=username, password=password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def download_file(self, remote_path, local_path):
        from pathlib import Path
        print(f"Téléchargement: {remote_path} -> {local_path}")
        with self.sftp.open(remote_path, 'rb') as r:
            content = r.read()
        Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)
        with open(local_path, 'wb') as w:
            w.write(content)
        print(f"✓ Succès: {len(content)} bytes transférés")

    def list_directory(self, path):  # fichiers simples
        try:
            attrs = self.sftp.listdir_attr(path)
            return [a.filename for a in attrs if stat.S_ISREG(a.st_mode)]
        except Exception as e:
            print(f"Erreur list_directory {path}: {e}")
            return []

    def change_directory(self, path):
        self.sftp.chdir(path)

    def get_current_directory(self):
        return self.sftp.getcwd()

    def return_to_root(self):
        cur = self.get_current_directory()
        depth = len([x for x in cur.split('/') if x]) if cur != "/" else 0
        if depth > 0: self.change_directory("../" * depth)

    def close(self):
        self.sftp.close(); self.transport.close()

def create_connection(mode, host="", port=22, username="", password="") -> Optional[ConnectionManager]:
    if mode == "ftp":  return FTPManager(host, username, password)
    if mode == "sftp": return SFTPManager(host, port, username, password)
    return None


# ============================================================================
# CHARGEMENT DES DONNÉES MINECRAFT
# ============================================================================

class MinecraftDataLoader:
    def __init__(self, mode, connection: Optional[ConnectionManager]=None,
                 ftp_path: str="", local_path: str=""):
        self.mode = mode
        self.connection = connection
        self.ftp_path = ftp_path
        self.local_path = local_path
        self.names_df = None

    def _get_paths(self) -> Dict[str, str]:
        if self.mode in ["ftp","sftp"]:
            return {
                'stats': "/Minecraft/world/stats",
                'playerdata': "/Minecraft/world/playerdata",
                'advancements': "/Minecraft/world/advancements",
                'usercache': "/Minecraft/usercache.json",
            }
        if self.mode == "local":
            return {
                'stats': f"{self.local_path}/world/stats/",
                'playerdata': f"{self.local_path}/world/playerdata/",
                'advancements': f"{self.local_path}/world/advancements/",
                'usercache': f"{self.local_path}/usercache.json",
            }
        return {
            'stats': 'data/stats',
            'playerdata': 'data/playerdata',
            'advancements': 'data/advancements',
            'usercache': 'data/usercache/usercache.json'
        }

    def _load_usercache(self):
        p = self._get_paths()
        if self.mode in ["ftp","sftp"]:
            self.connection.download_file(p['usercache'], "data/usercache/usercache.json")
            cache = "data/usercache/usercache.json"
        else:
            cache = p['usercache']
        with open(cache, 'r') as f:
            self.names_df = pd.DataFrame(json.load(f))

    def _name(self, uuid: str) -> str:
        s = self.names_df.loc[self.names_df['uuid'] == uuid]['name']
        return uuid if s.empty else s.iloc[0]

    def _process_stats_file(self, filepath: str, uuid: str) -> pd.DataFrame:
        with open(filepath, 'r') as f:
            data = json.load(f)
        tmp = pd.json_normalize(data, meta_prefix=True).transpose().iloc[1:]
        tmp = tmp.rename({0: self._name(uuid)}, axis=1)
        tmp.index = tmp.index.str.split('.', expand=True)
        if len(tmp.index.levshape) > 3:
            tmp.index = tmp.index.droplevel(3)
            tmp = tmp.groupby(level=[0, 1, 2]).sum()
        return tmp

    def _process_playerdata_file(self, filepath: str, uuid: str) -> Dict[str, any]:
        nbtfile = nbt.nbt.NBTFile(filepath, 'r')
        money = math.floor(nbtfile['cardinal_components']['numismatic-overhaul:currency']['Value'].value / 10000)
        waystones = len(nbtfile['BalmData']['WaystonesData']['Waystones'])
        return {'username': self._name(uuid), 'money': money, 'waystones': waystones}

    def _dl_many(self, remote_dir: str, local_dir: str, ext: str) -> List[str]:
        files = []
        for item in self.connection.list_directory(remote_dir):
            if not item.endswith(ext): continue
            uuid = item[:-len(ext)]
            remote = f"{remote_dir}/{item}"
            local = os.path.join(local_dir, item)
            self.connection.download_file(remote, local)
            files.append((local, uuid))
        return files

    def load_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        self._load_usercache()

        paths = self._get_paths()
        os.makedirs("data/stats", exist_ok=True)
        os.makedirs("data/playerdata", exist_ok=True)
        os.makedirs("data/advancements", exist_ok=True)

        if self.mode in ["ftp","sftp"]:
            stats_df = pd.DataFrame()
            for fp, uuid in self._dl_many(paths['stats'], "data/stats", ".json"):
                tmp = self._process_stats_file(fp, uuid)
                stats_df = tmp if stats_df.empty else stats_df.join(tmp, how="outer")

            money, way = {}, {}
            for fp, uuid in self._dl_many(paths['playerdata'], "data/playerdata", ".dat"):
                d = self._process_playerdata_file(fp, uuid)
                money[d['username']] = d['money']; way[d['username']] = d['waystones']
            money_df = pd.DataFrame(money, index=["money"]).transpose()
            way_df   = pd.DataFrame(way,   index=["waystones"]).transpose()

            adv_df = pd.DataFrame()
            for fp, uuid in self._dl_many(paths['advancements'], "data/advancements", ".json"):
                with open(fp, 'r') as f: data = json.load(f)
                tmp = pd.json_normalize(data, meta_prefix=True).transpose().iloc[1:]
                tmp = tmp.rename({0: self._name(uuid)}, axis=1)
                tmp.index = tmp.index.str.split('.', expand=True)
                tmp = tmp[~tmp.index.get_level_values(0).str.split(":", n=1).str[1].str.startswith("recipes")]
                adv_df = tmp if adv_df.empty else adv_df.join(tmp, how="outer")
        else:
            # mode local ou manuel (non utilisé ici mais on garde)
            stats_df = pd.DataFrame()
            for fn in os.listdir(paths['stats']):
                if not fn.endswith(".json"): continue
                tmp = self._process_stats_file(os.path.join(paths['stats'], fn), fn[:-5])
                stats_df = tmp if stats_df.empty else stats_df.join(tmp, how="outer")
            money_df = pd.DataFrame(); way_df = pd.DataFrame(); adv_df = pd.DataFrame()

        stats_df = stats_df.fillna(0); adv_df = adv_df.fillna(0)
        return stats_df, money_df, way_df, adv_df


# ============================================================================
# GÉNÉRATION DES LEADERBOARDS + IMAGE
# ============================================================================

class LeaderboardGenerator:
    @staticmethod
    def get_vanilla_leaderboard(df: pd.DataFrame, category: str, subcategory: str, verbose: bool=False) -> pd.DataFrame:
        if subcategory == "total":
            row = df.loc['stats'].loc[category].sum().sort_values().iloc[::-1]
        else:
            row = df.loc['stats'].loc[category].loc[subcategory].sort_values().iloc[::-1]
        out = pd.DataFrame(row).rename(columns={subcategory: 0})
        return out

    @staticmethod
    def get_advancements_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
        c = pd.DataFrame((df == True).sum().sort_values())
        c['index'] = range(len(c), 0, -1)
        return c.iloc[::-1]

class LeaderboardImageGenerator:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.github = None
        if config.get_bool('GITHUB','Enable', False):
            self.github = GitHubUploader(
                token=config.get('GITHUB','Token',''),
                repo_owner=config.get('GITHUB','RepoOwner',''),
                repo_name=config.get('GITHUB','RepoName',''),
                branch=config.get('GITHUB','Branch','main')
            )

    def _font(self, filename, size):
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts", filename)
        print(f"Loading font: {p}")
        if not os.path.exists(p):
            print("⚠️ Font introuvable -> police par défaut")
            return ImageFont.load_default()
        return ImageFont.truetype(p, size)

    def _gradient(self, w,h,c1,c2):
        base = Image.new('RGBA',(w,h),c1); top = Image.new('RGBA',(w,h),c2)
        mask = Image.new('L',(w,h))
        mask.putdata([int(255*(y/h)) for y in range(h) for _ in range(w)])
        base.paste(top,(0,0),mask); return base

    def _panel(self, draw,x,y,w,h, border=(139,90,43), bg=(50,50,50,230)):
        if isinstance(border,str): border = ImageColor.getrgb(border)
        if isinstance(bg,str): bg = ImageColor.getrgb(bg)
        draw.rectangle([x+4,y+4,x+w-4,y+h-4], fill=bg)
        draw.rectangle([x,y,x+w,y+3], fill=border); draw.rectangle([x,y+h-3,x+w,y+h], fill=border)
        draw.rectangle([x,y,x+3,y+h], fill=border); draw.rectangle([x+w-3,y,x+w,y+h], fill=border)

    def _badge(self, draw,x,y,rank,size=50):
        colors={1:('#FFD700','#FFA500'),2:('#C0C0C0','#808080'),3:('#CD7F32','#8B4513')}
        main,shadow = colors.get(rank,('#555','#333'))
        self._panel(draw,x,y,size,size, border=shadow, bg=main)
        f = self._font("Minecraft-Seven_v2.ttf", 22)
        t = str(rank); bbox = draw.textbbox((0,0), t, font=f)
        draw.text((x+(size-(bbox[2]-bbox[0]))//2, y+(size-(bbox[3]-bbox[1]))//2), t, fill='#fff', font=f)

    def generate_top_image(self, leaderboards: List[pd.DataFrame], titles: List[str]) -> Optional[str]:
        image_path = self.config.get("TOPIMAGE","ImagePath")  # images/top3.png
        df = leaderboards[0] if leaderboards else pd.DataFrame()
        title = titles[0] if titles else "Leaderboard"

        total_players = 30
        num_cols = 3
        per_col = total_players // num_cols
        extra = total_players % num_cols
        entry_h = 70
        base_w = 650
        base_h = 200 + (per_col * entry_h) - 70
        W = base_w * num_cols; H = base_h

        img = self._gradient(W,H,(15,15,35,0),(15,15,35,0))
        d = ImageDraw.Draw(img,'RGBA')

        # 3 colonnes
        start = 1
        for i in range(num_cols):
            end = start + per_col - 1 + (1 if i < extra else 0)
            x0 = i*base_w
            # panneau
            self._panel(d, x0+20, 20, base_w-40, base_h-40)
            # titre
            ftitle = self._font("Minecraft-Seven_v2.ttf", 28)
            text = f"{title} - RANKS {start}-{end}"
            tw = d.textlength(text, font=ftitle)
            d.text((x0 + (base_w-tw)//2, 40), text, fill="#FFD700", font=ftitle)

            # lignes
            y = 100
            for rank in range(start, end+1):
                # fond
                d.rectangle([x0+35, y, x0+base_w-55, y+entry_h-5], fill=(40 + 10*((rank-start)%2),)*3 + (210,))
                self._badge(d, x0+45, y+10, rank, 50)
                if rank <= len(df):
                    player = df.iloc[rank-1]
                    name = player.name
                    score = player.iloc[0]
                    if not isinstance(score, str): score = f"{int(score):,}"

                    # avatar
                    try:
                        r = requests.get(f"https://mc-heads.net/avatar/{name}")
                        av = Image.open(BytesIO(r.content)).resize((64,64), Image.Resampling.LANCZOS)
                        box = Image.new('RGBA',(68,68),(139,90,43))
                        box.paste(av,(2,2)); img.paste(box,(x0+105, y+3), box)
                    except:
                        d.rectangle([x0+105, y+3, x0+173, y+71], fill=(100,100,100))

                    fname = self._font("minecraft.ttf", 20)
                    fscore = self._font("Minecraft-Seven_v2.ttf", 18)
                    d.text((x0+185, y+12), name, fill="#FFFFFF", font=fname)
                    d.text((x0+185, y+40), f"Score: {score}", fill="#55FF55", font=fscore)
                else:
                    # slot vide
                    d.rectangle([x0+105, y+3, x0+173, y+71], outline=(60,60,60), fill=(30,30,30))
                    fname = self._font("minecraft.ttf", 20); fscore = self._font("Minecraft-Seven_v2.ttf", 18)
                    d.text((x0+185, y+12), "---", fill="#666", font=fname)
                    d.text((x0+185, y+40), "Score: ---", fill="#444", font=fscore)
                y += entry_h
            start = end + 1

        os.makedirs(os.path.dirname(image_path), exist_ok=True)
        img.save(image_path)
        print(f"✅ Modern Minecraft-style leaderboard saved to {image_path}")

        url = None
        if self.github:
            # commit vers images/top3.png
            url = self.github.upload_image(image_path, "images/top3.png")
            if not url:
                url = self.github.get_latest_commit_url("images/top3.png")
            self._save_url_files(url)
        return url

    def _save_url_files(self, url: str):
        try:
            with open('latest_leaderboard_url.txt','w') as f: f.write(url)
            # lien jsDelivr stable aussi
            owner = os.getenv('GITHUB_REPOSITORY_OWNER') or ""
            repo  = os.getenv('GITHUB_REPOSITORY','/').split('/')[-1] if os.getenv('GITHUB_REPOSITORY') else ""
            stable = f"https://cdn.jsdelivr.net/gh/{owner}/{repo}@main/images/top3.png" if owner and repo else url
            with open('LINK.md','w',encoding='utf-8') as f:
                f.write("# Latest Leaderboard\n\n")
                f.write(f"![Minecraft Leaderboard]({url})\n\n")
                f.write(f"**URL directe:** {url}\n\n")
                f.write(f"**Stable (jsDelivr):** {stable}\n")
        except Exception as e:
            print(f"⚠️  Impossible de sauvegarder les liens: {e}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("Loading configuration...")
    cfg = ConfigManager()

    # identifiants SFTP/FTP
    username = open("username.txt","r").read().strip()
    password = open("password.txt","r").read().strip()

    mode = cfg.get('INPUT','Mode')
    conn = None
    if mode in ("ftp","sftp"):
        conn = create_connection(
            mode,
            host=cfg.get('INPUT','Host'),
            port=cfg.get_int('INPUT','Port',22),
            username=username,
            password=password
        )

    try:
        print("\nLOADING VANILLA DATA")
        loader = MinecraftDataLoader(
            mode=mode,
            connection=conn,
            ftp_path=cfg.get('INPUT','FTPPath'),
            local_path=cfg.get('INPUT','LocalPath')
        )
        vanilla_df, money_df, way_df, adv_df = loader.load_all_data()

        if cfg.get_bool('VANILLALEADERBOARD','CreateCSV'):
            vanilla_df.to_csv(cfg.get('VANILLALEADERBOARD','CSVPath'))
            print("Stats saved to", cfg.get('VANILLALEADERBOARD','CSVPath'))

        lbs, titles = [], []
        for spec in cfg.get_list('TOPIMAGE','Leaderboards'):
            parts = spec.split('/')
            if parts[0] == "vanilla":
                if parts[1] == "advancements":
                    lb = LeaderboardGenerator.get_advancements_leaderboard(adv_df)
                else:
                    lb = LeaderboardGenerator.get_vanilla_leaderboard(vanilla_df, parts[1], parts[2])
                lbs.append(lb)
        titles = cfg.get_list('TOPIMAGE','Titles') or ["Leaderboard"]

        if cfg.get_bool('TOPIMAGE','Enable'):
            print("\nGenerating top leaderboards image...")
            url = LeaderboardImageGenerator(cfg).generate_top_image(lbs, titles)
            if url:
                print("\n🌐 Image:", url)
                print("🖼️  ![Leaderboard](", url, ")")
            else:
                print("❌ Aucun lien généré")
    finally:
        if conn:
            print("\nClosing connection...")
            conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
