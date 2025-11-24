import os
import random
import string
import cloudinary
import cloudinary.uploader
import cloudinary.api
import re
import io
import json
import sys
import requests
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, case, or_
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from functools import wraps
from collections import defaultdict, deque
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
from itertools import product, combinations

# --- 1. アプリケーションとデータベースの初期設定 ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY')
basedir = os.path.abspath(os.path.dirname(__file__))

cloudinary.config(
    cloud_name = os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key = os.environ.get('CLOUDINARY_API_KEY'),
    api_secret = os.environ.get('CLOUDINARY_API_SECRET')
)

database_url = os.environ.get('DATABASE_URL')
if database_url:
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url.replace("postgres://", "postgresql://", 1)
else:
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'database.db')

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- 2. ログインマネージャーの設定 ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = "このページにアクセスするにはログインが必要です。"

# --- 3. データベースモデル（テーブル）の定義 ---
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256))
    role = db.Column(db.String(20), nullable=False, default='user')
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)
    @property
    def is_admin(self): return self.role == 'admin'

class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    logo_image = db.Column(db.String(255), nullable=True)
    league = db.Column(db.String(50), nullable=True)
    players = db.relationship('Player', backref='team', lazy=True)

class Player(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)

class Game(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    game_date = db.Column(db.String(50))
    start_time = db.Column(db.String(20), nullable=True)
    game_password = db.Column(db.String(50), nullable=True)
    home_team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    away_team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    home_score = db.Column(db.Integer, default=0)
    away_score = db.Column(db.Integer, default=0)
    is_finished = db.Column(db.Boolean, default=False)
    youtube_url_home = db.Column(db.String(200), nullable=True)
    youtube_url_away = db.Column(db.String(200), nullable=True)
    winner_id = db.Column(db.Integer, nullable=True)
    loser_id = db.Column(db.Integer, nullable=True)
    # result_input_time は削除されました
    home_team = db.relationship('Team', foreign_keys=[home_team_id])
    away_team = db.relationship('Team', foreign_keys=[away_team_id])

class PlayerStat(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=False)
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    pts=db.Column(db.Integer, default=0); ast=db.Column(db.Integer, default=0)
    reb=db.Column(db.Integer, default=0); stl=db.Column(db.Integer, default=0)
    blk=db.Column(db.Integer, default=0); foul=db.Column(db.Integer, default=0)
    turnover=db.Column(db.Integer, default=0); fgm=db.Column(db.Integer, default=0)
    fga=db.Column(db.Integer, default=0); three_pm=db.Column(db.Integer, default=0)
    three_pa=db.Column(db.Integer, default=0); ftm=db.Column(db.Integer, default=0)
    fta=db.Column(db.Integer, default=0)
    player = db.relationship('Player')

class News(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow) 
    
    def __repr__(self):
        return f'<News {self.title}>'

# --- 4. 権限管理とヘルパー関数 ---
Team_Home = db.aliased(Team, name='team_home') 
Team_Away = db.aliased(Team, name='team_away')

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash("この操作には管理者権限が必要です。"); return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'png', 'jpg', 'jpeg', 'gif'}

def generate_password(length=4):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def calculate_standings(league_filter=None):
    if league_filter: teams = Team.query.filter_by(league=league_filter).all()
    else: teams = Team.query.all()
    standings = []
    for team in teams:
        wins, losses, points_for, points_against, stats_games_played = 0, 0, 0, 0, 0
        home_games = Game.query.filter_by(home_team_id=team.id, is_finished=True).all()
        for game in home_games:
            if game.winner_id is None:
                points_for += game.home_score; points_against += game.away_score; stats_games_played += 1
            if game.winner_id == team.id: wins += 1
            elif game.loser_id == team.id: losses += 1
            elif game.home_score > game.away_score: wins += 1
            elif game.home_score < game.away_score: losses += 1
        away_games = Game.query.filter_by(away_team_id=team.id, is_finished=True).all()
        for game in away_games:
            if game.winner_id is None:
                points_for += game.away_score; points_against += game.home_score; stats_games_played += 1
            if game.winner_id == team.id: wins += 1
            elif game.loser_id == team.id: losses += 1
            elif game.away_score > game.home_score: wins += 1
            elif game.away_score < game.home_score: losses += 1
        points = (wins * 2) + (losses * 1)
        standings.append({
            'team': team, 'team_name': team.name, 'league': team.league, 'wins': wins, 'losses': losses, 'points': points,
            'avg_pf': round(points_for / stats_games_played, 1) if stats_games_played > 0 else 0,
            'avg_pa': round(points_against / stats_games_played, 1) if stats_games_played > 0 else 0,
            'diff': points_for - points_against, 'stats_games_played': stats_games_played
        })
    standings.sort(key=lambda x: (x['points'], x['diff']), reverse=True)
    return standings

def get_stats_leaders():
    """スタッツリーダー（Top 5）を取得する"""
    leaders = {}
    stat_fields = {'pts': '平均得点', 'ast': '平均アシスト', 'reb': '平均リバウンド', 'stl': '平均スティール', 'blk': '平均ブロック'}
    for field_key, field_name in stat_fields.items():
        avg_stat = func.avg(getattr(PlayerStat, field_key)).label('avg_value')
        
        query_result = db.session.query(
            Player.name, avg_stat, Player.id 
        ).join(PlayerStat, PlayerStat.player_id == Player.id)\
         .group_by(Player.id)\
         .order_by(db.desc('avg_value'))\
         .limit(5).all()
        
        leaders[field_name] = query_result
    return leaders

def calculate_team_stats():
    team_stats_list = []
    standings_info = calculate_standings()
    shooting_stats_query = db.session.query(
        Player.team_id, func.sum(PlayerStat.pts).label('total_pts'),
        func.sum(PlayerStat.ast).label('total_ast'), func.sum(PlayerStat.reb).label('total_reb'),
        func.sum(PlayerStat.stl).label('total_stl'), func.sum(PlayerStat.blk).label('total_blk'),
        func.sum(PlayerStat.foul).label('total_foul'), func.sum(PlayerStat.turnover).label('total_turnover'),
        func.sum(PlayerStat.fgm).label('total_fgm'), func.sum(PlayerStat.fga).label('total_fga'),
        func.sum(PlayerStat.three_pm).label('total_3pm'), func.sum(PlayerStat.three_pa).label('total_3pa'),
        func.sum(PlayerStat.ftm).label('total_ftm'), func.sum(PlayerStat.fta).label('total_fta')
    ).join(Player).group_by(Player.team_id).all()
    shooting_map = {s.team_id: s for s in shooting_stats_query}
    for team_standings in standings_info:
        team_obj = team_standings.get('team')
        if not team_obj: continue
        stats_games_played = team_standings.get('stats_games_played', 0)
        team_shooting = shooting_map.get(team_obj.id)
        stats_dict = team_standings.copy()
        if stats_games_played > 0 and team_shooting:
            stats_dict.update({
                'avg_ast': team_shooting.total_ast / stats_games_played, 'avg_reb': team_shooting.total_reb / stats_games_played,
                'avg_stl': team_shooting.total_stl / stats_games_played, 'avg_blk': team_shooting.total_blk / stats_games_played,
                'avg_foul': team_shooting.total_foul / stats_games_played, 'avg_turnover': team_shooting.total_turnover / stats_games_played,
                'avg_fgm': team_shooting.total_fgm / stats_games_played, 'avg_fga': team_shooting.total_fga / stats_games_played,
                'avg_three_pm': team_shooting.total_3pm / stats_games_played, 'avg_three_pa': team_shooting.total_3pa / stats_games_played,
                'avg_ftm': team_shooting.total_ftm / stats_games_played, 'avg_fta': team_shooting.total_fta / stats_games_played,
                'fg_pct': (team_shooting.total_fgm / team_shooting.total_fga * 100) if team_shooting.total_fga > 0 else 0,
                'three_p_pct': (team_shooting.total_3pm / team_shooting.total_3pa * 100) if team_shooting.total_3pa > 0 else 0,
                'ft_pct': (team_shooting.total_ftm / team_shooting.total_fta * 100) if team_shooting.total_fta > 0 else 0,
            })
        team_stats_list.append(stats_dict)
    return team_stats_list

# ====================================
# ルーティング
# ====================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user is None or not user.check_password(request.form['password']):
            flash('ユーザー名またはパスワードが無効です'); return redirect(url_for('login'))
        login_user(user); return redirect(url_for('index'))
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user(); flash('ログアウトしました。'); return redirect(url_for('index'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form['username']
        if User.query.filter_by(username=username).first():
            flash("そのユーザー名は既に使用されています。"); return redirect(url_for('register'))
        role = 'admin' if User.query.count() == 0 else 'user'
        new_user = User(username=username, role=role)
        new_user.set_password(request.form['password'])
        db.session.add(new_user); db.session.commit()
        flash(f"ユーザー登録が完了しました。ログインしてください。"); return redirect(url_for('login'))
    return render_template('register.html')


# ------------------------------------
# ★★★ ルート定義の順序変更: 'schedule' は 'index' より上 ★★★
# ------------------------------------

@app.route('/schedule')
def schedule():
    selected_team_id = request.args.get('team_id', type=int)
    selected_date = request.args.get('selected_date')

    query = Game.query.order_by(Game.game_date.desc(), Game.start_time.desc())

    if selected_team_id:
        query = query.filter(
            (Game.home_team_id == selected_team_id) | (Game.away_team_id == selected_team_id)
        )
    if selected_date:
        query = query.filter(Game.game_date == selected_date)

    games = query.all()
    all_teams = Team.query.order_by(Team.name).all()

    return render_template('schedule.html', 
                           games=games, 
                           all_teams=all_teams, 
                           selected_team_id=selected_team_id,
                           selected_date=selected_date)

@app.route('/game/<int:game_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_game(game_id):
    game = Game.query.get_or_404(game_id)
    
    if request.method == 'POST':
        game.youtube_url_home = request.form.get('youtube_url_home'); 
        game.youtube_url_away = request.form.get('youtube_url_away')
        PlayerStat.query.filter_by(game_id=game_id).delete()
        
        home_total_score, away_total_score = 0, 0
        for team in [game.home_team, game.away_team]:
            for player in team.players:
                if f'player_{player.id}_pts' in request.form:
                    stat = PlayerStat(game_id=game.id, player_id=player.id); 
                    db.session.add(stat)
                    # ★★★ 修正: player_{player.id} を使用 ★★★
                    stat.pts = request.form.get(f'player_{player.id}_pts', 0, type=int); 
                    stat.ast = request.form.get(f'player_{player.id}_ast', 0, type=int)
                    stat.reb = request.form.get(f'player_{player.id}_reb', 0, type=int); 
                    stat.stl = request.form.get(f'player_{player.id}_stl', 0, type=int)
                    stat.blk = request.form.get(f'player_{player.id}_blk', 0, type=int); 
                    stat.foul = request.form.get(f'player_{player.id}_foul', 0, type=int)
                    stat.turnover = request.form.get(f'player_{player.id}_turnover', 0, type=int); 
                    stat.fgm = request.form.get(f'player_{player.id}_fgm', 0, type=int)
                    stat.fga = request.form.get(f'player_{player.id}_fga', 0, type=int); 
                    stat.three_pm = request.form.get(f'player_{player.id}_three_pm', 0, type=int)
                    stat.three_pa = request.form.get(f'player_{player.id}_three_pa', 0, type=int); 
                    stat.ftm = request.form.get(f'player_{player.id}_ftm', 0, type=int)
                    stat.fta = request.form.get(f'player_{player.id}_fta', 0, type=int)
                    
                    if team.id == game.home_team_id: 
                        home_total_score += stat.pts
                    else: 
                        away_total_score += stat.pts
                        
        game.home_score = home_total_score; 
        game.away_score = away_total_score
        game.is_finished = True; 
        game.winner_id = None; 
        game.loser_id = None
        
        # result_input_time の設定は削除 (リバート)
        
        db.session.commit()
        flash('試合結果が更新されました。'); 
        return redirect(url_for('game_result', game_id=game.id))
        
    # --- GETリクエスト (編集フォームの表示) ---
    stats = {
        str(stat.player_id): {
            'pts': stat.pts, 'reb': stat.reb, 'ast': stat.ast, 'stl': stat.stl, 'blk': stat.blk,
            'foul': stat.foul, 'turnover': stat.turnover, 'fgm': stat.fgm, 'fga': stat.fga,
            'three_pm': stat.three_pm, 'three_pa': stat.three_pa, 'ftm': stat.ftm, 'fta': stat.fta
        } for stat in PlayerStat.query.filter_by(game_id=game_id).all()
    }
    return render_template('game_edit.html', game=game, stats=stats)


# ... (roster, add_schedule, auto_schedule, etc. のルートが続く) ...


@app.route('/game/<int:game_id>/result')
def game_result(game_id):
    """
    新しい「試合結果閲覧」ページ
    """
    game = Game.query.get_or_404(game_id)
    
    # 試合結果のスタッツを取得
    stats = {
        str(stat.player_id): {
            'pts': stat.pts, 'reb': stat.reb, 'ast': stat.ast, 'stl': stat.stl, 'blk': stat.blk,
            'foul': stat.foul, 'turnover': stat.turnover, 'fgm': stat.fgm, 'fga': stat.fga,
            'three_pm': stat.three_pm, 'three_pa': stat.three_pa, 'ftm': stat.ftm, 'fta': stat.fta
        } for stat in PlayerStat.query.filter_by(game_id=game_id).all()
    }
    
    return render_template('game_result.html', game=game, stats=stats)


# ... (他のルートが続く) ...

@app.route('/roster', methods=['GET', 'POST'])
@login_required
@admin_required
def roster():
    # ... (roster関数のロジック) ...
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add_team':
            # ... (add_team の処理) ...
            team_name = request.form.get('team_name'); league = request.form.get('league')
            logo_url = None
            if 'logo_image' in request.files:
                file = request.files['logo_image']
                if file and file.filename != '' and allowed_file(file.filename):
                    try:
                        upload_result = cloudinary.uploader.upload(file); logo_url = upload_result.get('secure_url')
                    except Exception as e:
                        flash(f"画像アップロードに失敗しました: {e}"); return redirect(url_for('roster'))
                elif file.filename != '': flash('許可されていないファイル形式です。'); return redirect(url_for('roster'))
            if team_name and league:
                if not Team.query.filter_by(name=team_name).first():
                    new_team = Team(name=team_name, league=league, logo_image=logo_url)
                    db.session.add(new_team); db.session.commit()
                    flash(f'チーム「{team_name}」が{league}に登録されました。')
                    for i in range(1, 11):
                        player_name = request.form.get(f'player_name_{i}')
                        if player_name:
                            new_player = Player(name=player_name, team_id=new_team.id); db.session.add(new_player)
                    db.session.commit()
                else: flash(f'チーム「{team_name}」は既に存在します。')
            else: flash('チーム名とリーグを選択してください。')

        elif action == 'add_player':
            # ... (add_player の処理) ...
            player_name = request.form.get('player_name'); team_id = request.form.get('team_id')
            if player_name and team_id:
                new_player = Player(name=player_name, team_id=team_id)
                db.session.add(new_player); db.session.commit()
                flash(f'選手「{player_name}」が登録されました。')
            else: flash('選手名とチームを選択してください。')

        elif action == 'promote_user':
            # ... (promote_user の処理) ...
            username_to_promote = request.form.get('username_to_promote')
            if username_to_promote:
                user_to_promote = User.query.filter_by(username=username_to_promote).first()
                if user_to_promote:
                    if user_to_promote.role != 'admin':
                        user_to_promote.role = 'admin'; db.session.commit()
                        flash(f'ユーザー「{username_to_promote}」を管理者に昇格させました。')
                    else: flash(f'ユーザー「{username_to_promote}」は既に管理者です。')
                else: flash(f'ユーザー「{username_to_promote}」が見つかりません。')
            else: flash('ユーザー名を入力してください。')

        elif action == 'edit_player':
            # ... (edit_player の処理) ...
            player_id = request.form.get('player_id', type=int); new_name = request.form.get('new_name')
            player = Player.query.get(player_id)
            if player and new_name: player.name = new_name; db.session.commit(); flash(f'選手名を「{new_name}」に変更しました。')

        elif action == 'transfer_player':
            # ... (transfer_player の処理) ...
            player_id = request.form.get('player_id', type=int); new_team_id = request.form.get('new_team_id', type=int)
            player = Player.query.get(player_id); new_team = Team.query.get(new_team_id)
            if player and new_team:
                old_team_name = player.team.name
                player.team_id = new_team_id; db.session.commit()
                flash(f'選手「{player.name}」を{old_team_name}から{new_team.name}に移籍させました。')

        elif action == 'update_logo':
            # ... (update_logo の処理) ...
            team_id = request.form.get('team_id', type=int)
            team = Team.query.get(team_id)
            if not team:
                flash('対象のチームが見つかりません。')
                return redirect(url_for('roster'))
            if 'logo_image' in request.files:
                file = request.files['logo_image']
                if file and file.filename != '' and allowed_file(file.filename):
                    try:
                        if team.logo_image:
                            public_id = os.path.splitext(team.logo_image.split('/')[-1])[0]
                            cloudinary.uploader.destroy(public_id)
                        upload_result = cloudinary.uploader.upload(file)
                        logo_url = upload_result.get('secure_url')
                        team.logo_image = logo_url
                        db.session.commit()
                        flash(f'チーム「{team.name}」のロゴを更新しました。')
                    except Exception as e:
                        flash(f"ロゴの更新に失敗しました: {e}")
                elif file.filename != '':
                    flash('許可されていないファイル形式です。')
            else:
                flash('ロゴファイルが選択されていません。')

        elif action == 'add_news':
            # ... (add_news の処理) ...
            title = request.form.get('news_title')
            content = request.form.get('news_content')
            if title and content:
                new_item = News(title=title, content=content)
                db.session.add(new_item)
                db.session.commit()
                flash(f'お知らせ「{title}」を投稿しました。')
            else:
                flash('タイトルと内容の両方を入力してください。')

        elif action == 'delete_news':
            # ... (delete_news の処理) ...
            news_id_to_delete = request.form.get('news_id', type=int)
            news_item = News.query.get(news_id_to_delete)
            if news_item:
                db.session.delete(news_item)
                db.session.commit()
                flash('お知らせを削除しました。')
            else:
                flash('削除対象のニュースが見つかりません。')
        
        return redirect(url_for('roster'))

    teams = Team.query.all()
    users = User.query.all()
    news_items = News.query.order_by(News.created_at.desc()).all()
    
    return render_template('roster.html', 
                           teams=teams, 
                           users=users, 
                           news_items=news_items)

# ... (既存のその他のルートは省略) ...

@app.route('/stats') # stats ルートは index や schedule の上に定義されている前提
def stats_page():
    # ... (stats_page ロジックは省略) ...
    team_stats = calculate_team_stats()
    individual_stats = db.session.query(
        Player.id.label('player_id'), 
        Player.name.label('player_name'), 
        Team.id.label('team_id'),   
        Team.name.label('team_name'),
        func.count(PlayerStat.game_id).label('games_played'),
        func.avg(PlayerStat.pts).label('avg_pts'), 
        func.avg(PlayerStat.ast).label('avg_ast'),
        func.avg(PlayerStat.reb).label('avg_reb'), 
        func.avg(PlayerStat.stl).label('avg_stl'),
        func.avg(PlayerStat.blk).label('avg_blk'), 
        func.avg(PlayerStat.foul).label('avg_foul'),
        func.avg(PlayerStat.turnover).label('avg_turnover'), 
        func.avg(PlayerStat.fgm).label('avg_fgm'),
        func.avg(PlayerStat.fga).label('avg_fga'), 
        func.avg(PlayerStat.three_pm).label('avg_three_pm'),
        func.avg(PlayerStat.three_pa).label('avg_three_pa'), 
        func.avg(PlayerStat.ftm).label('avg_ftm'),
        func.avg(PlayerStat.fta).label('avg_fta'),
        case((func.sum(PlayerStat.fga) > 0, (func.sum(PlayerStat.fgm) * 100.0 / func.sum(PlayerStat.fga))), else_=0).label('fg_pct'),
        case((func.sum(PlayerStat.three_pa) > 0, (func.sum(PlayerStat.three_pm) * 100.0 / func.sum(PlayerStat.three_pa))), else_=0).label('three_p_pct'),
        case((func.sum(PlayerStat.fta) > 0, (func.sum(PlayerStat.ftm) * 100.0 / func.sum(PlayerStat.fta))), else_=0).label('ft_pct')
    ).join(Player, PlayerStat.player_id == Player.id)\
     .join(Team, Player.team_id == Team.id)\
     .group_by(Player.id, Team.id, Team.name)\
     .all()
    
    return render_template('stats.html', team_stats=team_stats, individual_stats=individual_stats)


@app.route('/game/<int:game_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_game(game_id):
    game = Game.query.get_or_404(game_id)
    
    if request.method == 'POST':
        game.youtube_url_home = request.form.get('youtube_url_home'); 
        game.youtube_url_away = request.form.get('youtube_url_away')
        PlayerStat.query.filter_by(game_id=game_id).delete()
        
        home_total_score, away_total_score = 0, 0
        for team in [game.home_team, game.away_team]:
            for player in team.players:
                if f'player_{player.id}_pts' in request.form:
                    stat = PlayerStat(game_id=game.id, player_id=player.id); 
                    db.session.add(stat)
                    stat.pts = request.form.get(f'player_{player.id}_pts', 0, type=int); 
                    stat.ast = request.form.get(f'player_{player.id}_ast', 0, type=int)
                    stat.reb = request.form.get(f'player_{player.id}_reb', 0, type=int); 
                    stat.stl = request.form.get(f'player_{player.id}_stl', 0, type=int)
                    stat.blk = request.form.get(f'player_{player.id}_blk', 0, type=int); 
                    stat.foul = request.form.get(f'player_{player.id}_foul', 0, type=int)
                    stat.turnover = request.form.get(f'player_{player.id}_turnover', 0, type=int); 
                    stat.fgm = request.form.get(f'player_{player.id}_fgm', 0, type=int)
                    stat.fga = request.form.get(f'player_{player.id}_fga', 0, type=int); 
                    stat.three_pm = request.form.get(f'player_{player.id}_three_pm', 0, type=int)
                    stat.three_pa = request.form.get(f'player_{player.id}_three_pa', 0, type=int); 
                    stat.ftm = request.form.get(f'player_{player.id}_ftm', 0, type=int)
                    stat.fta = request.form.get(f'player_{player.id}_fta', 0, type=int)
                    
                    if team.id == game.home_team_id: 
                        home_total_score += stat.pts
                    else: 
                        away_total_score += stat.pts
                        
        game.home_score = home_total_score; 
        game.away_score = away_total_score
        game.is_finished = True; 
        game.winner_id = None; 
        game.loser_id = None
        
        # result_input_time の設定は削除 (リバート)
        
        db.session.commit()
        flash('試合結果が更新されました。'); 
        return redirect(url_for('game_result', game_id=game.id))
        
    # --- GETリクエスト (編集フォームの表示) ---
    stats = {
        str(stat.player_id): {
            'pts': stat.pts, 'reb': stat.reb, 'ast': stat.ast, 'stl': stat.stl, 'blk': stat.blk,
            'foul': stat.foul, 'turnover': stat.turnover, 'fgm': stat.fgm, 'fga': stat.fga,
            'three_pm': stat.three_pm, 'three_pa': stat.three_pa, 'ftm': stat.ftm, 'fta': stat.fta
        } for stat in PlayerStat.query.filter_by(game_id=game_id).all()
    }
    return render_template('game_edit.html', game=game, stats=stats)

@app.route('/game/<int:game_id>/result')
def game_result(game_id):
    """
    ★★★ 新しい「試合結果閲覧」ページ ★★★
    誰でも閲覧可能。
    """
    game = Game.query.get_or_404(game_id)
    
    # 試合結果のスタッツを取得 (edit_gameと同じロジック)
    stats = {
        str(stat.player_id): {
            'pts': stat.pts, 'reb': stat.reb, 'ast': stat.ast, 'stl': stat.stl, 'blk': stat.blk,
            'foul': stat.foul, 'turnover': stat.turnover, 'fgm': stat.fgm, 'fga': stat.fga,
            'three_pm': stat.three_pm, 'three_pa': stat.three_pa, 'ftm': stat.ftm, 'fta': stat.fta
        } for stat in PlayerStat.query.filter_by(game_id=game_id).all()
    }
    
    # 新しい閲覧用テンプレートを呼び出す
    return render_template('game_result.html', game=game, stats=stats)


# ... (他のルートが続く) ...

# --- 6. データベース初期化コマンドと実行 ---
@app.cli.command('init-db')
def init_db_command():
    # db.drop_all() # <-- 既存のデータを消さないよう、ここはコメントアウト
    db.create_all()
    print('Initialized the database. (Existing tables were not dropped)')
if __name__ == '__main__':
    app.run(debug=True)