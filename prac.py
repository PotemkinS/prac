from datetime import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import random

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///mail_segments.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class UserSegments(db.Model):
    __tablename__ = 'user_segments'
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), primary_key=True)
    segment_id = db.Column(db.Integer, db.ForeignKey('segments.id'), primary_key=True)

class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    email = db.Column(db.String, unique=True, nullable=False)
    last_name = db.Column(db.String, nullable=False)
    first_name = db.Column(db.String, nullable=False)
    middle_name = db.Column(db.String, nullable=True)
    birth_date = db.Column(db.Date, nullable=True)
    gender = db.Column(db.String, nullable=True)
    segments = db.relationship('Segments', secondary='user_segments', back_populates='users')

class Segments(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)
    description = db.Column(db.Text, nullable=True)
    users = db.relationship('Users', secondary='user_segments', back_populates='segments')

@app.route('/user/add', methods=['POST'])
def add_user():
    data = request.json
    email = data.get('email')
    last_name = data.get('last_name')
    first_name = data.get('first_name')
    middle_name = data.get('middle_name')
    birth_date_str = data.get('birth_date')
    gender = data.get('gender')

    if not email or not last_name or not first_name:
        return {"error": "email, last_name and first_name are required"}, 400

    birth_date = None
    if birth_date_str:
        try:
            birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d').date()
        except ValueError:
            return {"error": "birth_date must be in YYYY-MM-DD format"}, 400

    if Users.query.filter_by(email=email).first():
        return {"error": "User with this email already exists"}, 400

    user = Users(
        email=email,
        last_name=last_name,
        first_name=first_name,
        middle_name=middle_name,
        birth_date=birth_date,
        gender=gender
    )
    db.session.add(user)
    db.session.commit()

    return {"message": "User added", "user_id": user.id}, 201

@app.route('/segment/add', methods=['POST'])
def add_segment():
    data = request.json
    name = data.get('name')
    description = data.get('description')

    if not name:
        return {"error": "name is required"}, 400

    segment = Segments(name=name, description=description)
    db.session.add(segment)
    db.session.commit()
    return {"message": "Segment added", "segment_id": segment.id}, 201

@app.route('/segment/change/<int:segment_id>', methods=['PUT'])
def update_segment(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404

    data = request.json
    segment.name = data.get('name', segment.name)
    if 'description' in data:
        segment.description = data['description']
    db.session.commit()
    return {"message": "Segment updated"}

@app.route('/segment/delete/<int:segment_id>', methods=['DELETE'])
def delete_segment(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404
    db.session.delete(segment)
    db.session.commit()
    return {"message": "Segment deleted"}

@app.route('/segment/add_users_by_ids/<int:segment_id>', methods=['POST'])
def add_segment_to_users_by_ids(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404

    user_ids = request.json.get('user_ids', [])
    if not isinstance(user_ids, list) or not user_ids:
        return {"error": "user_ids list required"}, 400

    users = Users.query.filter(Users.id.in_(user_ids)).all()
    added_count = 0
    for user in users:
        assoc = UserSegments.query.filter_by(user_id=user.id, segment_id=segment.id).first()
        if not assoc:
            assoc = UserSegments(user_id=user.id, segment_id=segment.id)
            db.session.add(assoc)
            added_count += 1
    db.session.commit()
    return {"message": f"Segment {segment_id} added to {added_count} users by IDs"}, 200

@app.route('/segment/add_users_by_percent/<int:segment_id>', methods=['POST'])
def add_segment_to_users_by_percent(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404

    percent = request.json.get('percent')
    if not isinstance(percent, (int, float)) or not (0.0 <= percent <= 100.0):
        return {"error": "percent must be between 0 and 100"}, 400

    all_users = Users.query.all()
    num_to_assign = int(len(all_users) * (percent / 100.0))
    selected_users = random.sample(all_users, num_to_assign) if num_to_assign > 0 else []

    added_count = 0
    for user in selected_users:
        assoc = UserSegments.query.filter_by(user_id=user.id, segment_id=segment.id).first()
        if not assoc:
            assoc = UserSegments(user_id=user.id, segment_id=segment.id)
            db.session.add(assoc)
            added_count += 1
    db.session.commit()
    return {"message": f"Segment {segment_id} added randomly to {added_count} users"}, 200

@app.route('/segment/add_users_by_param/<int:segment_id>', methods=['POST'])
def add_segment_to_users_by_param(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404

    param_name = request.json.get('param_name')
    param_value = request.json.get('param_value')

    if not param_name or param_value is None:
        return {"error": "param_name and param_value are required"}, 400

    if not hasattr(Users, param_name):
        return {"error": f"User has no attribute '{param_name}'"}, 400

    users = Users.query.filter(getattr(Users, param_name) == param_value).all()
    added_count = 0
    for user in users:
        assoc = UserSegments.query.filter_by(user_id=user.id, segment_id=segment.id).first()
        if not assoc:
            assoc = UserSegments(user_id=user.id, segment_id=segment.id)
            db.session.add(assoc)
            added_count += 1
    db.session.commit()
    return {"message": f"Segment {segment_id} added to {added_count} users where {param_name}='{param_value}'"}, 200

@app.route('/user/<int:user_id>/segments', methods=['GET'])
def get_segments_of_user(user_id):
    user = Users.query.get(user_id)
    if not user:
        return {"error": "User not found"}, 404

    segments = [{"id": seg.id, "name": seg.name, "description": seg.description} for seg in user.segments]
    return {"user_id": user_id, "segments": segments}

@app.route('/user/<int:user_id>', methods=['GET'])
def get_user_by_id(user_id):
    user = Users.query.get(user_id)
    if not user:
        return {"error": "User not found"}, 404

    user_data = {
        "id": user.id,
        "email": user.email,
        "last_name": user.last_name,
        "first_name": user.first_name,
        "middle_name": user.middle_name,
        "birth_date": user.birth_date.isoformat() if user.birth_date else None,
        "gender": user.gender
    }
    return {"user": user_data}

@app.route('/segment/<int:segment_id>', methods=['GET'])
def get_segment_by_id(segment_id):
    segment = Segments.query.get(segment_id)
    if not segment:
        return {"error": "Segment not found"}, 404

    segment_data = {
        "id": segment.id,
        "name": segment.name,
        "description": segment.description
    }
    return {"segment": segment_data}

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
