"""
Clean Scoresheet OCR Pipeline
These are the steps for the scoresheet OCR pipeline
1. YOLO Detection - detect number cells and move cells
2. Deskew - straighten image based on YOLO box orientation
3. Number Detection - via YOLO directly OR fallback to move-edge PaddleOCR
4. Number Extrapolation - fill gaps using monotonicity and y-axis alignment
5. Move Association - associate move boxes with numbers by y-position (one white + one black per row)
6. Handwritten OCR - run fine-tuned PaddleOCR on move boxes

"""

import os
import cv2
import numpy as np
import time
from PIL import Image
from ultralytics import YOLO
from paddleocr import PaddleOCR
import pytesseract
import paddle
from paddle import inference
import chess
from scipy.optimize import linear_sum_assignment

paddle.set_device('gpu:0')

#True means use PaddleOCR while False will give Tesseract
USE_PADDLE_FOR_NUMBERS = True

#Fine tuned Paddle OCR model using HCS
class PaddleRecognizer:
    
    def __init__(self, 
                 model_dir='PaddleOCR/output/chess_scoresheet/inference',
                 dict_path='data/chess_dict_full.txt'):
        
        # Load character dictionary
        with open(dict_path, 'r') as f:
            self.chars = [line.strip() for line in f.readlines()]
        self.chars = [''] + self.chars  # Add blank at index 0
        
        # Create predictor
        model_file = os.path.join(model_dir, 'inference.json')
        params_file = os.path.join(model_dir, 'inference.pdiparams')
        
        config = inference.Config(model_file, params_file)
        config.enable_use_gpu(500, 0)
        config.disable_glog_info()
        
        self.predictor = inference.create_predictor(config)
        
        self.input_names = self.predictor.get_input_names()
        self.output_names = self.predictor.get_output_names()
        
        self.img_shape = [3, 48, 320]  
    
    def preprocess(self, img):
        if img is None or img.size == 0:
            return None
        
        if len(img.shape) == 2:
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
        
        # Resize maintaining aspect ratio
        h, w = img.shape[:2]
        ratio = self.img_shape[1] / h
        new_w = int(w * ratio)
        if new_w > self.img_shape[2]:
            new_w = self.img_shape[2]
        
        resized = cv2.resize(img, (new_w, self.img_shape[1]))
        
        # pad to target width with white (255) to match training data
        padded = np.ones((self.img_shape[1], self.img_shape[2], 3), dtype=np.float32) * 255
        padded[:, :new_w, :] = resized
        
        padded = padded.astype(np.float32) / 255.0
        padded = (padded - 0.5) / 0.5
        padded = padded.transpose((2, 0, 1))  
        
        return padded[np.newaxis, :, :, :]  

    #decode the ctc to text    
    def decode(self, preds):
        pred_idx = preds.argmax(axis=2)[0]
        
        chars = []
        prev = 0
        for idx in pred_idx:
            if idx != prev and idx != 0 and idx <= len(self.chars) - 1:
                chars.append(self.chars[idx])
            prev = idx
        
        return ''.join(chars)
    
    def recognize(self, img):
        input_data = self.preprocess(img)
        if input_data is None:
            return '', 0.0
        
        # Set input
        input_handle = self.predictor.get_input_handle(self.input_names[0])
        input_handle.reshape(input_data.shape)
        input_handle.copy_from_cpu(input_data)
        
        # inference
        self.predictor.run()
        
        output_handle = self.predictor.get_output_handle(self.output_names[0])
        output = output_handle.copy_to_cpu()
        
        text = self.decode(output)
        
        # use softmax to get confidence
        probs = output[0]
        max_probs = probs.max(axis=1)
        conf = float(np.mean(max_probs[max_probs > 0.1])) if len(max_probs[max_probs > 0.1]) > 0 else 0.0
        
        return text, conf

    #get text with beam search and get top-K candidates
    def beam_search_recognize(self, img, beam_width=20, top_k=5):
        input_data = self.preprocess(img)
        if input_data is None:
            return [('', 0.0)]
        
        input_handle = self.predictor.get_input_handle(self.input_names[0])
        input_handle.reshape(input_data.shape)
        input_handle.copy_from_cpu(input_data)
        self.predictor.run()
        
        output_handle = self.predictor.get_output_handle(self.output_names[0])
        probs = output_handle.copy_to_cpu()[0]  # Shape: (time_steps, num_classes)
        
        # beam search using log probs
        log_probs = np.log(probs + 1e-10)
        beams = [{'text': '', 'score': 0.0, 'last': -1}]
        
        for t in range(log_probs.shape[0]):
            new_beams = []
            for beam in beams:
                for idx in range(len(self.chars)):
                    lp = log_probs[t, idx]
                    if idx == 0:  # blank
                        new_beams.append({'text': beam['text'], 'score': beam['score'] + lp, 'last': idx})
                    elif idx != beam['last']:
                        new_beams.append({'text': beam['text'] + self.chars[idx], 'score': beam['score'] + lp, 'last': idx})
                    else:
                        new_beams.append({'text': beam['text'], 'score': beam['score'] + lp, 'last': idx})
            
            # erge same (text, last), keep best
            merged = {}
            for b in new_beams:
                key = (b['text'], b['last'])
                if key not in merged or b['score'] > merged[key]:
                    merged[key] = b['score']
            
            beams = [{'text': k[0], 'score': v, 'last': k[1]} for k, v in merged.items()]
            beams.sort(key=lambda x: -x['score'])
            beams = beams[:beam_width]
        
        # Get unique texts with best scores
        unique = {}
        for b in beams:
            if b['text'] not in unique or b['score'] > unique[b['text']]:
                unique[b['text']] = b['score']
        
        # onvert log probs to normalized probs
        results = [(text, np.exp(score)) for text, score in sorted(unique.items(), key=lambda x: -x[1])[:top_k]]
        total = sum(p for _, p in results)
        if total > 0:
            results = [(t, p/total) for t, p in results]
        
        return results


class ScoresheetPipeline:
    def __init__(self, yolo_path='runs/detect/multiclass_v15_hpad40/weights/best.pt',
                 finetuned_ocr_path='PaddleOCR/output/chess_black_only/inference',
                 dict_path='data/chess_dict_full.txt'):
        self.yolo = YOLO(yolo_path)
        # Pretrained OCR for number detection
        self.ocr = PaddleOCR(lang='en')
        # Fine-tuned OCR for chess move recognition
        self.move_ocr = PaddleRecognizer(model_dir=finetuned_ocr_path, dict_path=dict_path)
        print("done loading")
    
    def process(self, img_path, zoom=2):
        timings = {}
        starttime = time.time()
        
        # Load image
        pil_img = Image.open(img_path).convert('RGB')
        img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
        h_orig, w_orig = img.shape[:2]
        
        if zoom > 1:
            img = cv2.resize(img, (w_orig * zoom, h_orig * zoom), interpolation=cv2.INTER_CUBIC)
        
        h, w = img.shape[:2]
        print(f"Image: {w_orig}x{h_orig} -> {w}x{h} (zoom={zoom}x)")
        
        #Step1: YOLO detection
        print("\n" + "="*60)
        print("Step 1: YOLO Detection")
        print("="*60)
        
        t0 = time.time()
        num_boxes, move_boxes = self.step1_yolo_detection(img)
        timings['step1_yolo'] = time.time() - t0
        print(f"  Detected: {len(num_boxes)} number boxes, {len(move_boxes)} move boxes")
        print(f"  Time: {timings['step1_yolo']:.2f}s")
        
        #2: deskew ===========
        print("Step 2: Deskew")
        
        t0 = time.time()
        img, angle = self.step2_deskew(img, move_boxes)
        if abs(angle) > 0.1:
            print(f"  Rotated image by {angle:.2f}° to straighten")
            # run YOLO again after deskew to get corrected box positions
            num_boxes, move_boxes = self.step1_yolo_detection(img)
            print(f" found: {len(num_boxes)} number boxes, {len(move_boxes)} move boxes")
        else:
            print(f"  Image already straight (angle={angle:.2f}°)")
        timings['step2_deskew'] = time.time() - t0
        print(f"  Time: {timings['step2_deskew']:.2f}s")
        
        h, w = img.shape[:2]  # Update dimensions after rotation
        
        #Step 3: NUMBER DETECTION + LAYOUT ===
        print("\n" + "="*60)
        print("STEP 3: Number Detection + Column Layout")
        print("="*60)
        
        t0 = time.time()
        numbers, num_columns, layout = self.step3_number_detection(img, num_boxes, move_boxes, w)
        timings['step3_numbers'] = time.time() - t0
        print(f"  Layout: {num_columns} move columns ({layout})")
        print(f"  Detected: {len(numbers)} numbers")
        print(f"  Time: {timings['step3_numbers']:.2f}s")
        
        #step 4 - number extrapolation
        print("step 4: number extrapolation")
        
        t0 = time.time()
        numbers = self.step4_extrapolate_numbers(numbers)
        timings['step4_extrap'] = time.time() - t0
        print(f"  After extrapolation: {len(numbers)} numbers")
        print(f"  Time: {timings['step4_extrap']:.2f}s")
        
        # step 5 - associate moves
        print("step 5: associate moves")
        
        t0 = time.time()
        rows = self.step5_associate_moves(numbers, move_boxes)
        timings['step5_assoc'] = time.time() - t0
        matched_white = sum(1 for r in rows.values() if r.get('white_box'))
        matched_black = sum(1 for r in rows.values() if r.get('black_box'))
        print(f"  Matched: {matched_white} white, {matched_black} black")
        print(f"  Time: {timings['step5_assoc']:.2f}s")
        
        #step 6: ocr 
        print("\n" + "="*60)
        print("step 6 : ocr")
        print("="*60)
        
        t0 = time.time()
        rows = self.step6_ocr_moves(img, rows)
        timings['step6_ocr'] = time.time() - t0
        found_white = sum(1 for r in rows.values() if r.get('white_text'))
        found_black = sum(1 for r in rows.values() if r.get('black_text'))
        print(f"  Found: {found_white} white, {found_black} black moves")
        print(f"  Time: {timings['step6_ocr']:.2f}s")
        
        #timing results
        total_time = time.time() - starttime
        print("\n" + "="*60)
        print("timing summary")
        print("="*60)
        for step, t in sorted(timings.items()):
            pct = (t / total_time) * 100
            print(f"  {step:20s}: {t:6.2f}s ({pct:5.1f}%)")
        print(f"  {'TOTAL':20s}: {total_time:6.2f}s")
        
        return rows, img
    
    #step 1 - run yolo and find number/move cells
    def step1_yolo_detection(self, img):
        results = self.yolo(img, conf=0.25, verbose=False)
        
        num_boxes = []
        move_boxes = []
        
        for b in results[0].boxes:
            x1, y1, x2, y2 = map(int, b.xyxy[0].cpu().numpy())
            cls = int(b.cls)
            conf = float(b.conf)
            
            box = {
                'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2,
                'cx': (x1 + x2) // 2, 'cy': (y1 + y2) // 2,
                'w': x2 - x1, 'h': y2 - y1,
                'conf': conf
            }
            
            if cls == 0:  # number
                num_boxes.append(box)
            else:  # move
                move_boxes.append(box)
        
        return num_boxes, move_boxes
    
    #step 2 - dedkew the image to reduce orientation angle
    def step2_deskew(self, img, move_boxes):
        if len(move_boxes) < 10:
            return img, 0.0
        
        h, w = img.shape[:2]
        
        # wecan use box centers but only from the SAME ROW (horizontally aligned boxes)
        # Sort boxes by Y, group into rows, then check if boxes in same row are horizontally aligned
        sorted_by_y = sorted(move_boxes, key=lambda m: m['cy'])
        
        heights = [m['y2'] - m['y1'] for m in move_boxes]
        median_height = np.median(heights)
        
        # Group boxes into rows
        rows = []
        current_row = [sorted_by_y[0]]
        
        for i in range(1, len(sorted_by_y)):
            if sorted_by_y[i]['cy'] - sorted_by_y[i-1]['cy'] > median_height * 0.5:
                rows.append(current_row)
                current_row = [sorted_by_y[i]]
            else:
                current_row.append(sorted_by_y[i])
        rows.append(current_row)
        
        # For each row with multiple boxes, compute the angle between leftmost and rightmost box
        # Only use first 10 rows to avoid multi-column perspective distortion
        row_angles = []
        for row in rows[:10]:  # Limit to first 10 rows
            if len(row) < 2:
                continue
            
            # Sort row by x
            row_sorted = sorted(row, key=lambda m: m['cx'])
            left_box = row_sorted[0]
            right_box = row_sorted[-1]
            
            # Compute angle between box centers
            dx = right_box['cx'] - left_box['cx']
            dy = right_box['cy'] - left_box['cy']
            
            if dx < 100:  # Boxes too close
                continue
            
            angle_deg = np.degrees(np.arctan2(dy, dx))
            row_angles.append(angle_deg)
        
        if not row_angles:
            return img, 0.0
        
        # Take the 5 angles closest to 0° (most vertical rows) and use their median
        angles_arr = np.array(row_angles)
        sorted_by_abs = angles_arr[np.argsort(np.abs(angles_arr))]
        smallest_5 = sorted_by_abs[:min(5, len(sorted_by_abs))]
        skew_angle = np.median(smallest_5)
        
        # Only deskew if angle is significant (> 1.0 degrees)
        if abs(skew_angle) < 1.0:
            return img, skew_angle
        
        # Rotate image to correct skew
        # Positive angle means right side is lower, so rotate counter-clockwise (positive in cv2)
        center = (w // 2, h // 2)
        rotation_matrix = cv2.getRotationMatrix2D(center, skew_angle, 1.0)
        
        # Calculate new image size to avoid clipping
        cos = abs(rotation_matrix[0, 0])
        sin = abs(rotation_matrix[0, 1])
        new_w = int(h * sin + w * cos)
        new_h = int(h * cos + w * sin)
        
        # Adjust rotation matrix for new center
        rotation_matrix[0, 2] += (new_w - w) / 2
        rotation_matrix[1, 2] += (new_h - h) / 2
        
        rotated = cv2.warpAffine(img, rotation_matrix, (new_w, new_h), 
                                  borderMode=cv2.BORDER_REPLICATE)
        
        return rotated, skew_angle
    
    #step 3 - find the column layout
    def step3_number_detection(self, img, num_boxes, move_boxes, img_width):
        numbers = []
        
        # First, detect column layout from move boxes
        num_columns, layout, columns = self.detect_column_layout(move_boxes)
        
        # Method 1: Use YOLO number boxes with OCR
        if num_boxes:
            if USE_PADDLE_FOR_NUMBERS:
                print("  Method A: Using YOLO number boxes with PaddleOCR...")
                numbers = self.ocr_number_boxes_paddle(img, num_boxes)
            else:
                print("  Method A: Using YOLO number boxes with Tesseract...")
                numbers = self.ocr_number_boxes_tesseract(img, num_boxes)
            
            print(f"    Detected: {len(numbers)} numbers")
        
        # Check if we have at least 5 numbers
        if len(numbers) >= 5:
            print("  Sufficient numbers from YOLO, skipping fallback")
            return self.deduplicate_numbers(numbers), num_columns, layout
        
        # Fallback - find number regions from move boxes and run PaddleOCR
        print("  Method B: Fallback - using move box edges for number detection...")
        fallback_numbers = self.fallback_number_detection(img, move_boxes, img_width, columns)
        print(f"  Fallback detected: {len(fallback_numbers)} numbers")
        
        # Merge results (prefer YOLO if same number detected)
        numbers.extend(fallback_numbers)
        return self.deduplicate_numbers(numbers), num_columns, layout

    # do number ocr using tesseract
    def ocr_number_boxes_tesseract(self, img, num_boxes):
        numbers = []
        for box in num_boxes:
            crop = img[box['y1']:box['y2'], box['x1']:box['x2']]
            if crop.size == 0:
                continue
            
            gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
            _, proc = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            text = pytesseract.image_to_string(proc, config='--psm 7 -c tessedit_char_whitelist=0123456789').strip()
            
            digits = ''.join(c for c in text if c.isdigit())
            if digits:
                try:
                    num = int(digits)
                    if 1 <= num <= 60:
                        numbers.append({
                            'number': num,
                            'y': box['cy'],
                            'x': box['cx'],
                            'box': box,
                            'source': 'yolo+tesseract'
                        })
                except:
                    pass
        return numbers
    
    # do number ocr using paddleOCR
    def ocr_number_boxes_paddle(self, img, num_boxes):
        numbers = []
        for box in num_boxes:
            crop = img[box['y1']:box['y2'], box['x1']:box['x2']]
            if crop.size == 0:
                continue
            
            result = self.ocr.predict(crop)
            if result and result[0]:
                rec_texts = result[0].get('rec_texts', [])
                if rec_texts:
                    text = rec_texts[0]
                    digits = ''.join(c for c in text if c.isdigit())
                    if digits:
                        try:
                            num = int(digits)
                            if 1 <= num <= 60:
                                numbers.append({
                                    'number': num,
                                    'y': box['cy'],
                                    'x': box['cx'],
                                    'box': box,
                                    'source': 'yolo+paddleocr'
                                })
                        except:
                            pass
        return numbers

    
    def detect_column_layout(self, move_boxes):
        """Detect column layout from move boxes.
        
        Returns:
            num_columns: number of move columns (2, 4, or 6)
            layout: description string
            columns: list of column box groups
        """
        if not move_boxes:
            return 2, "2-col (default)", []
        
        # Cluster move boxes by x-position
        move_widths = [m['x2'] - m['x1'] for m in move_boxes]
        avg_width = np.median(move_widths) if move_widths else 100
        
        sorted_moves = sorted(move_boxes, key=lambda m: m['cx'])
        columns = []
        current_col = [sorted_moves[0]]
        
        for i in range(1, len(sorted_moves)):
            if sorted_moves[i]['cx'] - sorted_moves[i-1]['cx'] > avg_width * 0.7:
                columns.append(current_col)
                current_col = [sorted_moves[i]]
            else:
                current_col.append(sorted_moves[i])
        columns.append(current_col)
        
        # Filter significant columns (at least 3 moves)
        columns = [c for c in columns if len(c) >= 3]
        
        num_columns = len(columns)
        
        if num_columns <= 2:
            layout = "2-col (WHITE + BLACK)"
        elif num_columns <= 4:
            layout = "4-col (2 number columns)"
        else:
            layout = "6-col (3 number columns)"
        
        return num_columns, layout, columns
    
    def fallback_number_detection(self, img, move_boxes, img_width, columns=None):
        """Fallback: detect numbers from vertical strips using median x1 per column.
        
        For each column: calculate median x1, then create strip from 
        (median_x1 - 50% of move width) to (median_x1 + 10% of move width).
        Run PaddleOCR on each strip.
        """
        if not move_boxes:
            return []
        
        numbers = []
        h = img.shape[0]
        
        # Strip params based on move box dimensions
        move_widths = [m['x2'] - m['x1'] for m in move_boxes]
        max_move_width = max(move_widths)
        avg_move_width = np.median(move_widths)
        
        strip_left_extend = int(max_move_width * 0.5)  # 50% left of median x1
        strip_overlap = int(max_move_width * 0.1)      # 10% overlap into move box
        
        # Compute columns if not provided
        if columns is None or len(columns) == 0:
            sorted_moves = sorted(move_boxes, key=lambda m: m['cx'])
            columns = []
            current_col = [sorted_moves[0]]
            
            for i in range(1, len(sorted_moves)):
                if sorted_moves[i]['cx'] - sorted_moves[i-1]['cx'] > avg_move_width * 0.5:
                    columns.append(current_col)
                    current_col = [sorted_moves[i]]
                else:
                    current_col.append(sorted_moves[i])
            columns.append(current_col)
            
            # Filter significant columns (at least 3 moves)
            columns = [c for c in columns if len(c) >= 3]
        
        if not columns:
            return []
        
        # For each column, create strip based on MEDIAN x1
        for col_idx, col in enumerate(columns):
            # Get median x1 for this column (robust to outliers)
            x1_values = [m['x1'] for m in col]
            median_x1 = int(np.median(x1_values))
            
            # Strip: from (median_x1 - 50% of move width) to (median_x1 + 10% overlap)
            region_start = max(0, median_x1 - strip_left_extend)
            region_end = min(img_width, median_x1 + strip_overlap)
            
            if region_end > region_start + 10:
                region = img[:, region_start:region_end]
                found = self.ocr_number_region(region, region_start)
                numbers.extend(found)
        
        return numbers
    
    def ocr_number_region(self, region, x_offset):
        """Run PaddleOCR on a number region."""
        numbers = []
        
        result = self.ocr.predict(region)
        if result and result[0]:
            rec_texts = result[0].get('rec_texts', [])
            rec_scores = result[0].get('rec_scores', [])
            rec_polys = result[0].get('rec_polys', [])
            
            for txt, score, poly in zip(rec_texts, rec_scores, rec_polys):
                if score < 0.5:
                    continue
                    
                digits = ''.join(c for c in txt if c.isdigit())
                if digits:
                    try:
                        num = int(digits)
                        if 1 <= num <= 60:
                            y_coords = [p[1] for p in poly]
                            y_center = (min(y_coords) + max(y_coords)) / 2
                            x_coords = [p[0] for p in poly]
                            x_center = x_offset + (min(x_coords) + max(x_coords)) / 2
                            
                            numbers.append({
                                'number': num,
                                'y': y_center,
                                'x': x_center,
                                'source': 'paddleocr_fallback'
                            })
                    except:
                        pass
        
        return numbers
    
    def deduplicate_numbers(self, numbers):
        """Remove duplicate numbers, keeping highest confidence."""
        seen = {}
        for n in numbers:
            num = n['number']
            if num not in seen:
                seen[num] = n
            elif n.get('source') == 'yolo+tesseract':
                # Prefer YOLO source
                seen[num] = n
        return sorted(seen.values(), key=lambda x: x['number'])
    
    #step 4
    def step4_extrapolate_numbers(self, numbers):
        """
        Fill in missing numbers using monotonicity and row spacing.
        Ensures that missing numbers at column boundaries (e.g., 16, 17) 
        are placed in the correct vertical strip.
        """
        if len(numbers) < 2:
            print("  Not enough numbers for extrapolation")
            return numbers
        
        # Estimate row spacing based on detected numbers
        row_spacing = self.estimate_row_spacing(numbers)
        print(f"  Row spacing: {row_spacing:.1f}px")
        
        # --- OUTLIER FILTERING ---
        # Filter out numbers whose Y position is inconsistent with their ordinal position
        # This catches misread numbers (e.g., "1" detected at bottom of scoresheet)
        columns_pre = self.group_by_columns(numbers)
        filtered_numbers = []
        
        for col_numbers in columns_pre:
            if len(col_numbers) < 3:
                filtered_numbers.extend(col_numbers)
                continue
            
            # Get median Y for this column
            sorted_by_num = sorted(col_numbers, key=lambda n: n['number'])
            
            for n in col_numbers:
                # Find expected Y based on neighbors
                prev_nums = [x for x in sorted_by_num if x['number'] < n['number']]
                next_nums = [x for x in sorted_by_num if x['number'] > n['number']]
                
                # Estimate expected Y
                if prev_nums and next_nums:
                    prev_n = prev_nums[-1]  # Closest lower
                    next_n = next_nums[0]   # Closest higher
                    if next_n['number'] != prev_n['number']:
                        ratio = (n['number'] - prev_n['number']) / (next_n['number'] - prev_n['number'])
                        expected_y = prev_n['y'] + ratio * (next_n['y'] - prev_n['y'])
                    else:
                        expected_y = n['y']
                elif prev_nums:
                    prev_n = prev_nums[-1]
                    expected_y = prev_n['y'] + (n['number'] - prev_n['number']) * row_spacing
                elif next_nums:
                    next_n = next_nums[0]
                    expected_y = next_n['y'] - (next_n['number'] - n['number']) * row_spacing
                else:
                    expected_y = n['y']
                
                # Check if actual Y is within tolerance (3x row spacing)
                deviation = abs(n['y'] - expected_y)
                if deviation < row_spacing * 3:
                    filtered_numbers.append(n)
                else:
                    print(f"  [Outlier] Filtered number {n['number']}: y={n['y']:.0f}, expected ~{expected_y:.0f}, deviation={deviation:.0f}")
        
        numbers = filtered_numbers
        
        # Group numbers by column based on X-coordinate binning
        columns = self.group_by_columns(numbers)
        
        # Filter out columns with too few numbers (likely misdetections)
        MIN_COLUMN_SIZE = 5
        columns = [col for col in columns if len(col) >= MIN_COLUMN_SIZE]
        if not columns:
            # Fallback: keep all if filtering removed everything
            columns = self.group_by_columns(numbers)
        
        print(f"  Number columns: {len(columns)}")
        
        result = []
        all_detected = sorted([n['number'] for n in numbers])
        global_max = max(all_detected)
        
        # Track the end of the previous column to start the next one correctly
        prev_end_tracker = 0
        
        for col_idx, col_numbers in enumerate(columns):
            if not col_numbers:
                continue
            
            # Anchor used to estimate Y positions for missing moves in this strip
            anchor = min(col_numbers, key=lambda n: n['y'])
            
            # --- PERIODICITY-BASED BOUNDARY LOGIC ---
            # Determine the standard layout (e.g., 20 rows per column) based on the first transition
            # and apply it uniformly to all columns.
            
            # 1. Determine the split size (rows per column)
            if not hasattr(self, '_cached_row_split'):
                # Default to end of detected numbers if single column
                if len(columns) == 1:
                    max_num = max(n['number'] for n in columns[0])
                    # Snap to nearest 10
                    self.cached_row_split = ((max_num + 9) // 10) * 10
                else:
                    # Look at the gap between Col 0 max and Col 1 min
                    c0_max = max(n['number'] for n in columns[0])
                    c1_min = min(n['number'] for n in columns[1])
                    
                    # Find the best standard split candidate
                    candidates = [15, 20, 25, 30, 35, 40, 50, 60]
                    best_split = c0_max # Fallback
                    
                    # Logic: Split must be >= c0_max (contain all col 0 nums)
                    # and < c1_min (so col 1 starts after it)
                    valid = [c for c in candidates if c >= c0_max and c < c1_min]
                    
                    if valid:
                        best_split = valid[0] # Smallest valid (closest to c_0 max)
                        print(f"  [Extrapolation] Detected standard split: {best_split} rows/col (Gap {c0_max}-{c1_min})")
                    else:
                         # Fallback if gap is weird: just use c0_max or midpoint
                         best_split = c0_max
                         print(f"  [Extrapolation] Non-standard gap {c0_max}-{c1_min}. Using {best_split}")
                    
                    self.cached_row_split = best_split

            row_split = self.cached_row_split
            
            # 2. Apply the split
            col_start = (col_idx * row_split) + 1
            col_end = (col_idx + 1) * row_split
            
            # Adjustment for last column to include max detected if needed
            if col_idx == len(columns) - 1:
                col_end = max(col_end, global_max)

            print(f"    Column {col_idx}: range {col_start}-{col_end}")
            
            # --- EXTRAPOLATION LOOP ---
            # Sort detected numbers in this column for interpolation
            sorted_detected = sorted(col_numbers, key=lambda n: n['number'])
            
            for num in range(col_start, col_end + 1):
                existing = next((n for n in col_numbers if n['number'] == num), None)
                if existing:
                    result.append(existing)
                else:
                    # Interpolation Logic: Find neighbors
                    prev_n = max((n for n in sorted_detected if n['number'] < num), key=lambda n: n['number'], default=None)
                    next_n = min((n for n in sorted_detected if n['number'] > num), key=lambda n: n['number'], default=None)
                    
                    if prev_n and next_n:
                         # Linear Interpolation
                         ratio = (num - prev_n['number']) / (next_n['number'] - prev_n['number'])
                         y_est = prev_n['y'] + ratio * (next_n['y'] - prev_n['y'])
                         x_est = prev_n['x'] + ratio * (next_n['x'] - prev_n['x']) # Also interpolate X drift
                    elif prev_n:
                        # Extrapolate forward from last detected
                        y_est = prev_n['y'] + (num - prev_n['number']) * row_spacing
                        x_est = prev_n['x'] # Assume constant X for tail
                    elif next_n:
                        # Extrapolate backward from first detected
                        y_est = next_n['y'] - (next_n['number'] - num) * row_spacing
                        x_est = next_n['x']
                    else:
                         # No detected numbers in this col? Fallback to anchor
                         y_est = anchor['y'] + (num - anchor['number']) * row_spacing
                         x_est = anchor['x']

                    result.append({
                        'number': num,
                        'y': y_est,
                        'x': x_est,
                        'source': 'extrapolated'
                    })
        
        # Final pass to ensure Y-positions are strictly increasing
        result = self.check_monotonicity(result)
        
        return sorted(result, key=lambda x: x['number'])
    
    def estimate_row_spacing(self, numbers):
        """Estimate row spacing from detected numbers."""
        spacings = []
        sorted_nums = sorted(numbers, key=lambda x: x['number'])
        
        for i in range(len(sorted_nums) - 1):
            n1, n2 = sorted_nums[i], sorted_nums[i+1]
            num_diff = n2['number'] - n1['number']
            y_diff = n2['y'] - n1['y']
            
            if num_diff > 0 and y_diff > 0:
                spacings.append(y_diff / num_diff)
        
        return np.median(spacings) if spacings else 48
    
    def group_by_columns(self, numbers):
        """Group numbers into columns using X-coordinate histogram binning."""
        if not numbers:
            return []
        
        # Sort by X position
        sorted_nums = sorted(numbers, key=lambda n: n['x'])
        x_values = [n['x'] for n in sorted_nums]
        
        # Find gaps in X to identify column boundaries
        # A gap > 100px between consecutive numbers indicates a new column
        GAP_THRESHOLD = 100
        
        columns = []
        current_col = [sorted_nums[0]]
        
        for i in range(1, len(sorted_nums)):
            gap = sorted_nums[i]['x'] - sorted_nums[i-1]['x']
            if gap > GAP_THRESHOLD:
                # Start new column
                columns.append(current_col)
                current_col = []
            current_col.append(sorted_nums[i])
        columns.append(current_col)
        
        # If two adjacent columns have 80%+ overlap in numbers, keep only the rightmost
        if len(columns) >= 2:
            deduped_columns = []
            i = 0
            while i < len(columns):
                if i + 1 < len(columns):
                    nums1 = set(n['number'] for n in columns[i])
                    nums2 = set(n['number'] for n in columns[i + 1])
                    overlap = len(nums1 & nums2)
                    union = len(nums1 | nums2)
                    overlap_ratio = overlap / union if union > 0 else 0
                    
                    if overlap_ratio > 0.8:
                        # Columns have 80%+ same numbers - keep rightmost (usually the main one)
                        print(f"    [Dedup] Merging duplicate columns: overlap={overlap_ratio:.1%}")
                        deduped_columns.append(columns[i + 1])
                        i += 2  # Skip both, added the right one
                        continue
                
                deduped_columns.append(columns[i])
                i += 1
            columns = deduped_columns
        
        # Debug output
        print(f"    [Binning] Found {len(columns)} number column(s):")
        for i, col in enumerate(columns):
            x_min = min(n['x'] for n in col)
            x_max = max(n['x'] for n in col)
            nums = sorted([n['number'] for n in col])
            print(f"      Col {i}: x={x_min:.0f}-{x_max:.0f}, nums={nums}")
        
        # Sort each column by number
        for i in range(len(columns)):
            columns[i] = sorted(columns[i], key=lambda x: x['number'])
        
        return columns
    
    def check_monotonicity(self, numbers):
        """Validate that y-positions increase monotonically with number within each column."""
        # Group by x again
        columns = self.group_by_columns(numbers)
        
        result = []
        for col in columns:
            sorted_col = sorted(col, key=lambda x: x['y'])
            
            # Numbers should increase with y
            corrected = []
            for i, n in enumerate(sorted_col):
                expected_num = sorted_col[0]['number'] + i
                if n['number'] != expected_num:
                    # Correct the number
                    n = n.copy()
                    n['number'] = expected_num
                    n['source'] = 'monotonicity_corrected'
                corrected.append(n)
            
            result.extend(corrected)
        
        return result


    #step 4 helper
    def cluster_into_vertical_strips(self, boxes):
        """Clusters move boxes into vertical lanes based on X-order."""
        if not boxes: return []
        sorted_boxes = sorted(boxes, key=lambda b: b['cx'])
        
        # Gap threshold to separate move lanes. 
        gap_threshold = 65 
        
        strips = []
        current_strip = [sorted_boxes[0]]
        for i in range(1, len(sorted_boxes)):
            if sorted_boxes[i]['cx'] - sorted_boxes[i-1]['cx'] > gap_threshold:
                strips.append(current_strip)
                current_strip = []
            current_strip.append(sorted_boxes[i])
        strips.append(current_strip)
        
        return [{
            'boxes': s, 
            'x_center': np.median([b['cx'] for b in s]),
            'avg_width': np.median([b['w'] for b in s]),
            'avg_height': np.median([b['h'] for b in s])
        } for s in strips]



    #step 5
    #
    #Links move strips to number columns by first identifying the page topology.
    #    Enforces 2 strips per number column and fills all Step 3 rows.
    def step5_associate_moves(self, numbers, move_boxes):
        """
        """
        if not numbers: return {}

        # 1. remove header boxes above the first number
        min_num_y = min(n['y'] for n in numbers)
        valid_boxes = [b for b in move_boxes if b['cy'] >= (min_num_y - 20)]
        
        # 2.find strips of moves
        raw_lanes = self.cluster_into_vertical_strips(valid_boxes)
        # filter tiny noise strips (< 2 boxes)
        all_move_strips = sorted([s for s in raw_lanes if len(s['boxes']) >= 2], key=lambda s: s['x_center'])
        number_cols = sorted(self.group_by_columns(numbers), key=lambda c: np.median([n['x'] for n in c]))
        
        row_h = self.estimate_row_spacing(numbers)
        rows = {n['number']: {'white_box': None, 'black_box': None, 'y': n['y']} for n in numbers}

        # 3. do histgram check
        first_num_x = np.median([n['x'] for n in number_cols[0]])
        first_move_x = all_move_strips[0]['x_center'] if all_move_strips else 0
        
        # Format is White-Number-Black if a move strip exists to the left of the numbers
        is_split_format = first_move_x < (first_num_x - 40)
        print(f"\n[TOPOLOGY] Identified Format: {'[W][N][B]' if is_split_format else '[N][W][B]'}")

        # assign blocks sequentially
        # Map 2 strips to every 1 number column in order
        for i, ns_nums in enumerate(number_cols):
            start_idx = i * 2
            end_idx = start_idx + 2
            assigned_lanes = all_move_strips[start_idx:end_idx]
            
            ns_x = np.median([n['x'] for n in ns_nums])
            num_range = f"{ns_nums[0]['number']}-{ns_nums[-1]['number']}"
            print(f"  Block {i} ({num_range}): Strips {list(range(start_idx, min(end_idx, len(all_move_strips))))}")

            # 4. assign lanes to blocks
            white_lane, black_lane = None, None
            if len(assigned_lanes) >= 2:
                l0, l1 = sorted(assigned_lanes, key=lambda s: s['x_center'])
                if is_split_format:
                    # [W: Lane 0] [N] [B: Lane 1]
                    white_lane = l0 if l0['x_center'] < ns_x else None
                    black_lane = l1 if l1['x_center'] > ns_x else l1
                else:
                    # [N] [W: Lane 0] [B: Lane 1]
                    white_lane, black_lane = l0, l1
            elif len(assigned_lanes) == 1:
                white_lane = assigned_lanes[0]

            # 5. assign lanes to rows
            for n in ns_nums:
                num_num = n['number']
                num_y = n['y']

                for ctype, lane in [('white', white_lane), ('black', black_lane)]:
                    if not lane: continue
                    
                    # Find box in THIS lane closest to THIS number's Y
                    best_box = min(lane['boxes'], key=lambda b: abs(b['cy'] - num_y), default=None)
                    
                    if best_box and abs(best_box['cy'] - num_y) < row_h * 0.5:
                        rows[num_num][f"{ctype}_box"] = best_box
                    else:
                        # 6. EXTRAPOLATION: Create virtual box for e5, d4, etc.
                        rows[num_num][f"{ctype}_box"] = {
                            'cx': lane['x_center'], 'cy': num_y,
                            'w': lane['avg_width'], 'h': lane['avg_height'],
                            'source': 'virtual_fill',
                            'x1': int(lane['x_center'] - lane['avg_width']/2),
                            'x2': int(lane['x_center'] + lane['avg_width']/2),
                            'y1': int(num_y - lane['avg_height']/2),
                            'y2': int(num_y + lane['avg_height']/2)
                        }
        return rows

    #step 6
    # run paddle ocr on each move box and also do chess validation
    # use beam search to get multiple candidates and then validate using
    # against hte current board position using python-chess
    # also increaes the width and height a bit to get a better read
    def step6_ocr_moves(self, img, rows):
        h, w = img.shape[:2]
        board = chess.Board() 
        
        # check number of consecnutivey invalid notations
        consecutive_invalid = 0
        # Stop after 2 consecutive zero-confidence black moves
        MAX_CONSECUTIVE_INVALID = 2  
        # Below this, treat as junk or game ended
        MIN_OCR_CONFIDENCE = 0.3 
        
        # do moves in order for validation
        for num in sorted(rows.keys()):
            row = rows[num]
            
            # Check for early stopping
            if consecutive_invalid >= MAX_CONSECUTIVE_INVALID:
                # Remove remaining rows - game has ended
                for remove_num in sorted(rows.keys()):
                    if remove_num >= num:
                        rows[remove_num]['white_text'] = ''
                        rows[remove_num]['black_text'] = ''
                break
            
            white_valid_san = False
            black_valid_san = False
            white_conf = 0.0
            black_conf = 0.0
            
            # OCR and validate white move
            if row.get('white_box'):
                crop = self.get_expanded_crop(img, row['white_box'], h)
                candidates = self.ocr_single_box(crop, return_candidates=True)
                
                best_text = ''
                if candidates:
                    raw_text, raw_prob = candidates[0]
                    white_conf = float(raw_prob)  # Store for early stopping check
                    
                    # First, try to validate the raw OCR text directly (no expansion)
                    direct_move = None
                    try:
                        direct_move = board.parse_san(raw_text)
                        white_valid_san = True  # Valid SAN syntax
                    except:
                        pass
                    
                    if direct_move and direct_move in board.legal_moves:
                        # Raw OCR validates directly - use it
                        best_text = board.san(direct_move)
                        board.push(direct_move)
                        white_valid_san = True
                    elif raw_prob > 0.95:
                        # High confidence but doesn't validate - trust OCR anyway
                        best_text = raw_text
                        # Check if it's at least valid SAN syntax
                        try:
                            board.parse_san(raw_text)
                            white_valid_san = True
                        except:
                            pass
                    else:
                        # Try candidates with expansion
                        for text, prob in candidates:
                            move = self.try_parse_move(board, text)
                            if move:
                                best_text = board.san(move)
                                board.push(move)
                                white_valid_san = True
                                break
                        else:
                            best_text = raw_text
                
                rows[num]['white_text'] = best_text
                rows[num]['white_candidates'] = candidates[:5]
            
            # OCR and validate black move
            if row.get('black_box'):
                crop = self.get_expanded_crop(img, row['black_box'], h)
                candidates = self.ocr_single_box(crop, return_candidates=True)
                
                best_text = ''
                if candidates:
                    raw_text, raw_prob = candidates[0]
                    black_conf = float(raw_prob)  # Store for early stopping check
                    
                    # First, try to validate the raw OCR text directly (no expansion)
                    direct_move = None
                    try:
                        direct_move = board.parse_san(raw_text)
                        black_valid_san = True  # Valid SAN syntax
                    except:
                        pass
                    
                    if direct_move and direct_move in board.legal_moves:
                        # Raw OCR validates directly - use it
                        best_text = board.san(direct_move)
                        board.push(direct_move)
                        black_valid_san = True
                    elif raw_prob > 0.95:
                        # High confidence but doesn't validate - trust OCR anyway
                        best_text = raw_text
                        # Check if it's at least valid SAN syntax
                        try:
                            board.parse_san(raw_text)
                            black_valid_san = True
                        except:
                            pass
                    else:
                        # Try candidates with expansion
                        for text, prob in candidates:
                            move = self.try_parse_move(board, text)
                            if move:
                                best_text = board.san(move)
                                board.push(move)
                                black_valid_san = True
                                break
                        else:
                            best_text = raw_text
                
                rows[num]['black_text'] = best_text
                rows[num]['black_candidates'] = candidates[:5]
            
            # Update consecutive invalid counter
            # Stop after 2 consecutive zero-confidence black moves (indicates game ended)
            if black_conf == 0.0:
                consecutive_invalid += 1
            else:
                consecutive_invalid = 0  # Reset if we see any black move with confidence
        
        return rows

    #expanded crop resizeto match training ratio    
    def get_expanded_crop(self, img, box, h):
        w_img = img.shape[1]
        # Expand by 10px height, 3px width on each side
        y1 = max(0, box['y1'] - 10)
        y2 = min(h, box['y2'] + 10)
        x1 = max(0, box['x1'] - 3)
        x2 = min(w_img, box['x2'] + 3)
        crop = img[y1:y2, x1:x2]
        
        if crop.size > 0:
            # Resize to match training data dimensions
            # Training is around907x170 avg so weneed height=170 with same aspect ratio
            target_height = 170
            crop_h, crop_w = crop.shape[:2]
            scale = target_height / crop_h
            target_width = int(crop_w * scale)
            crop = cv2.resize(crop, (target_width, target_height), 
                              interpolation=cv2.INTER_CUBIC)
        return crop

    #check if OCR returned valid chess move    
    def try_parse_move(self, board, text):
        if not text or text == '-' or len(text) < 2:
            return None
        
        text = text.strip()
        
        # skip numbers obviously wrong
        if text.isdigit():
            return None
        
        #  list of candidate strings to try
        candidates = [text]
        
        # ensure if OCR doesn't get castling correct, wefix
        if text.startswith('O') or text.startswith('0') or text.startswith('o'):
            # Normalize castling - only accept complete O-O or O-O-O
            if text in ['O-O', 'o-o', '0-0']:
                candidates = ['O-O']
            elif text in ['O-O-O', 'o-o-o', '0-0-0']:
                candidates = ['O-O-O']
            elif text == 'O-' or text == '0-' or text == 'o-':
                # Partial castling - try completing it
                candidates = ['O-O', 'O-O-O']
            else:
                # Unknown castling variant - skip
                return None
        else:
            # Add common OCR fixes to candidates
            candidates.extend([
                text.replace('l', '1'),
                text.replace('I', '1'),
                text + '+',  # Missing check
                text + '#',  # Missing mate
            ])
        
        for cand in candidates:
            try:
                move = board.parse_san(cand)
                if move in board.legal_moves:
                    return move
            except ValueError as e:
                # Handle ambiguous moves like "Nd2" -> try "Nbd2", "Nfd2", etc.
                if 'ambiguous' in str(e).lower() and len(cand) >= 2:
                    piece = cand[0]
                    rest = cand[1:]
                    # Try adding file disambiguation (a-h)
                    for file_char in 'abcdefgh':
                        try:
                            disambig = piece + file_char + rest
                            move = board.parse_san(disambig)
                            if move in board.legal_moves:
                                return move
                        except:
                            pass
                    # Try adding rank disambiguation (1-8)
                    for rank_char in '12345678':
                        try:
                            disambig = piece + rank_char + rest
                            move = board.parse_san(disambig)
                            if move in board.legal_moves:
                                return move
                        except:
                            pass
            except:
                pass
        
        return None

    # do finetune inference on a single crop, get list of (text,prob)  
    # or just text depnding on return_candidates
    def ocr_single_box(self, crop, return_candidates=False):
        try:
            if return_candidates:
                candidates = self.move_ocr.beam_search_recognize(crop, beam_width=30, top_k=10)
                return candidates
            else:
                text, conf = self.move_ocr.recognize(crop)
                return text
        except Exception as e:
            return [('', 0.0)] if return_candidates else ''
    
    def visualize(self, img, rows, output_path):
        viz = img.copy()
        
        for num, row in rows.items():
            y = int(row['y'])            
            cv2.putText(viz, str(num), (10, y), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
            
            if row.get('white_box'):
                wb = row['white_box']
                cv2.rectangle(viz, (wb['x1'], wb['y1']), (wb['x2'], wb['y2']), (255, 100, 0), 2)            
            if row.get('black_box'):
                bb = row['black_box']
                cv2.rectangle(viz, (bb['x1'], bb['y1']), (bb['x2'], bb['y2']), (0, 100, 255), 2)
        
        cv2.imwrite(output_path, viz)
        print(f"\nSaved: {output_path}")


def main():
    import sys
    img_path = sys.argv[1] if len(sys.argv) > 1 else 'test_image_1.webp'
    
    pipeline = ScoresheetPipeline()
    
    rows, img = pipeline.process(img_path, zoom=2)
    pipeline.visualize(img, rows, 'viz_clean_pipeline.jpg')
    
    print(f"Final: Total rows: {len(rows)}")
    
    sources = {}
    for r in rows.values():
        src = r.get('source', 'unknown')
        sources[src] = sources.get(src, 0) + 1
    print(f"Number sources: {sources}")
    
    white = sum(1 for r in rows.values() if r.get('white_box'))
    black = sum(1 for r in rows.values() if r.get('black_box'))
    print(f"Moves: {white} white, {black} black")


if __name__ == '__main__':
    main()
