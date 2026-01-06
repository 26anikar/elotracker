from flask import Flask, request, jsonify
from flask_cors import CORS
import tempfile, os, io, base64, sys, re
import chess.pgn, chess
from datetime import datetime
import cv2
import numpy as np
from PIL import Image

sys.path.insert(0, '.')
from scoresheet_pipeline import ScoresheetPipeline as SP

app = Flask(__name__)

CORS(app, resources={
    r"/*": {
        "origins": [
            "https://images.elotracker.com",
            "https://elotracker.com",
            "https://www.elotracker.com",
            "http://localhost:*",
            "http://127.0.0.1:*"
        ],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

pipe = SP()

def fix_mv(txt, b):
    # try to match valid moves if ocr messed up
    legals = [b.san(m) for m in b.legal_moves]
    
    if txt in legals: return txt, 1.0
    
    # common ocr fails
    subs = [
        (r'^a([xX]?[a-h][1-8])', r'R\1'), # a -> R
        (r'^b([xX]?[a-h][1-8])', r'B\1'), # b -> B
        (r'l', '1'),
        (r'0-0-0', 'O-O-O'),
        (r'0-0', 'O-O'),
        (r'^[Oo0]-$', 'O-O'),
        (r'^([KQRBN])([a-h])([1-8])$', r'\1x\2\3'), # miss capture
    ]
    
    for pat, rep in subs:
        fixed = re.sub(pat, rep, txt)
        if fixed != txt and fixed in legals:
            return fixed, 0.9
    
    return None, 0

def gen_pgn(rows):
    game = chess.pgn.Game()
    game.headers["Event"] = "OCR"
    game.headers["Date"] = datetime.now().strftime("%Y.%m.%d")
    game.headers["White"] = "?"
    game.headers["Black"] = "?"
    game.headers["Result"] = "*"
    
    b = chess.Board()
    node = game
    vals = []
    fix_cnt = 0
    
    for n in sorted(rows.keys()):
        row = rows[n]
        ent = {'move_number': n, 'white': None, 'black': None}
        
        # white
        wt = row.get('white_text', '')
        if wt and wt != '-':
            ok = False
            real = wt
            
            try:
                m = b.parse_san(wt)
                if m in b.legal_moves:
                    node = node.add_variation(m)
                    b.push(m)
                    ok = True
            except: pass
            
            if not ok:
                f, score = fix_mv(wt, b)
                if f:
                    try:
                        m = b.parse_san(f)
                        node = node.add_variation(m)
                        b.push(m)
                        real = f
                        ok = True
                        fix_cnt += 1
                        print(f" fixed: {wt} -> {f}")
                    except: pass
            
            if ok:
                ent['white'] = {'text': real, 'valid': True, 'original': wt if real != wt else None}
            else:
                ent['white'] = {'text': wt, 'valid': False, 'reason': 'err'}
        
        # black
        bt = row.get('black_text', '')
        if bt and bt != '-':
            ok = False
            real = bt
            
            try:
                m = b.parse_san(bt)
                if m in b.legal_moves:
                    node = node.add_variation(m)
                    b.push(m)
                    ok = True
            except: pass
            
            if not ok:
                f, score = fix_mv(bt, b)
                if f:
                    try:
                        m = b.parse_san(f)
                        node = node.add_variation(m)
                        b.push(m)
                        real = f
                        ok = True
                        fix_cnt += 1
                        print(f" fixed: {bt} -> {f}")
                    except: pass
            
            if ok:
                ent['black'] = {'text': real, 'valid': True, 'original': bt if real != bt else None}
            else:
                ent['black'] = {'text': bt, 'valid': False, 'reason': 'err'}
        
        vals.append(ent)
    
    if fix_cnt > 0: print(f"total fixes: {fix_cnt}")
    
    return str(game), vals

def get_move_details(rows):
    w_list = []
    b_list = []
    keys = sorted(rows.keys())
    
    for n in keys:
        row = rows[n]
        
        # process white
        w_cands = []
        raw_w = row.get('white_candidates', [])
        # grab top 5
        top_w = raw_w[:5]
        
        for item in top_w:
            txt = item[0]
            val = float(item[1])
            w_cands.append((txt, val))
            
        w_obj = {
            'move_number': n,
            'text': row.get('white_text', ''),
            'candidates': w_cands
        }
        w_list.append(w_obj)

        # process black
        b_cands = []
        raw_b = row.get('black_candidates', [])
        top_b = raw_b[:5]
        
        for item in top_b:
            txt = item[0]
            val = float(item[1])
            b_cands.append((txt, val))
            
        b_obj = {
            'move_number': n,
            'text': row.get('black_text', ''),
            'candidates': b_cands
        }
        b_list.append(b_obj)
        
    return w_list, b_list

@app.route('/ocr', methods=['POST'])
def process():
    if 'image' not in request.files: return jsonify({'error': 'no img'}), 400
    
    f = request.files['image']
    if f.filename == '': return jsonify({'error': 'empty fname'}), 400
    
    inc_mv = request.form.get('include_moves', 'false').lower() == 'true'
    inc_viz = request.form.get('include_viz', 'false').lower() == 'true'
    zoom = int(request.form.get('zoom', 2))
    
    tmp_path = None
    try:
        # save temp
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp:
            f.save(tmp.name)
            tmp_path = tmp.name
        
        # viz img load
        img = cv2.imread(tmp_path)
        
        print("running pipeline...")
        res = pipe.process(tmp_path, zoom=zoom)
        rows = res[0]
        img = res[1]
        
        # make pgn
        pgn, vals = gen_pgn(rows)
        
        # stats calculation
        tot = 0
        val_cnt = 0
        
        for m in vals:
            has_mv = False
            if m['white'] or m['black']:
                has_mv = True
                tot += 1
            
            w_ok = False
            if m['white'] and m['white'].get('valid'):
                w_ok = True
                
            b_ok = False
            if m['black'] and m['black'].get('valid'):
                b_ok = True
                
            if w_ok or b_ok:
                val_cnt += 1

        inv_cnt = tot - val_cnt
        acc = 0
        if tot > 0:
            acc = (val_cnt / tot) * 100
            
        print(f"acc: {acc:.1f}% ({val_cnt}/{tot})")
        
        w_rec = 0
        b_rec = 0
        for r in rows.values():
            if r.get('white_text'): w_rec += 1
            if r.get('black_text'): b_rec += 1

        resp = {
            'pgn': pgn,
            'moves_validation': vals,
            'total_moves': len(rows),
            'white_recognized': w_rec,
            'black_recognized': b_rec,
            'valid_moves': val_cnt,
            'invalid_moves': inv_cnt,
            'accuracy_pct': round(acc, 1),
            'ocr_method': 'paddle'
        }
        
        if inc_mv:
            w_mvs, b_mvs = get_move_details(rows)
            resp['white_moves'] = w_mvs
            resp['black_moves'] = b_mvs
        
        if inc_viz:
            v_img = img.copy()
            for n, r in rows.items():
                # w box
                if r.get('white_box'):
                    b = r['white_box']
                    cv2.rectangle(v_img, (b['x1'], b['y1']), (b['x2'], b['y2']), (0, 255, 0), 2)
                    t = r.get('white_text', '')
                    if t: cv2.putText(v_img, t, (b['x1'], b['y1']-5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)
                
                # b box
                if r.get('black_box'):
                    b = r['black_box']
                    cv2.rectangle(v_img, (b['x1'], b['y1']), (b['x2'], b['y2']), (255, 0, 0), 2)
                    t = r.get('black_text', '')
                    if t: cv2.putText(v_img, t, (b['x1'], b['y1']-5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 0, 0), 1)
                
                # num
                cv2.putText(v_img, str(n), (10, int(r['y'])), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)
            
            ret, buf = cv2.imencode('.png', v_img)
            b64 = base64.b64encode(buf).decode('utf-8')
            resp['visualization'] = f"data:image/png;base64,{b64}"
        
        os.unlink(tmp_path)
        return jsonify(resp)
    
    except Exception as e:
        if tmp_path:
            try: os.unlink(tmp_path)
            except: pass
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("Start server :8080")
    app.run(host='0.0.0.0', port=8080, debug=False)