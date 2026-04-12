
import customtkinter as ctk
import tkinter as tk
from blockchain_engine import MVPEngine
from datetime import datetime, timedelta
import json
import os
import sys

# Set appearance
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

class CustomEntry(ctk.CTkEntry):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Use a single handler for all Control+Key events to support all layouts
        self._entry.bind("<Control-KeyPress>", self._on_control_key)
        self._entry.bind("<Button-3>", self.do_popup)

    def _on_control_key(self, event):
        # Physical keycodes for Windows: A=65, C=67, V=86
        # This works regardless of the active language layout
        if event.keycode == 86: # V key
            return self.manual_paste()
        elif event.keycode == 67: # C key
            return self.manual_copy()
        elif event.keycode == 65: # A key
            return self.manual_select_all()
        
        # Fallback for other platforms or if keycode differs
        key = event.keysym.lower()
        if key in ['v', 'м', 'cyrillic_em']:
            return self.manual_paste()
        elif key in ['c', 'с', 'cyrillic_es']:
            return self.manual_copy()
        elif key in ['a', 'ф', 'cyrillic_ef']:
            return self.manual_select_all()
        
        return None

    def manual_paste(self, event=None):
        try:
            text = self.winfo_toplevel().clipboard_get()
            try:
                if self._entry.selection_get():
                    self._entry.delete("sel.first", "sel.last")
            except:
                pass
            self._entry.insert("insert", text)
        except Exception:
            pass
        return "break"

    def manual_copy(self, event=None):
        try:
            selection = self._entry.selection_get()
            if selection:
                self.clipboard_clear()
                self.clipboard_append(selection)
        except Exception:
            text = self._entry.get()
            if text:
                self.clipboard_clear()
                self.clipboard_append(text)
        return "break"

    def manual_select_all(self, event=None):
        self._entry.select_range(0, 'end')
        self._entry.icursor('end')
        return "break"

    def do_popup(self, event):
        m = tk.Menu(self, tearoff=0)
        m.add_command(label="Копировать", command=self.manual_copy)
        m.add_command(label="Вставить", command=self.manual_paste)
        m.add_command(label="Выделить всё", command=self.manual_select_all)
        m.tk_popup(event.x_root, event.y_root)

class CustomTextbox(ctk.CTkTextbox):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._textbox.bind("<Control-KeyPress>", self._on_control_key)
        self._textbox.bind("<Button-3>", self.do_popup)

    def _on_control_key(self, event):
        # Physical keycodes for Windows: A=65, C=67, V=86
        if event.keycode == 86: # V key
            return self.manual_paste()
        elif event.keycode == 67: # C key
            return self.manual_copy()
        elif event.keycode == 65: # A key
            return self.manual_select_all()
        
        # Fallback
        key = event.keysym.lower()
        if key in ['v', 'м', 'cyrillic_em']:
            return self.manual_paste()
        elif key in ['c', 'с', 'cyrillic_es']:
            return self.manual_copy()
        elif key in ['a', 'ф', 'cyrillic_ef']:
            return self.manual_select_all()
        return None

    def manual_paste(self, event=None):
        try:
            text = self.winfo_toplevel().clipboard_get()
            self.insert("insert", text)
        except Exception:
            pass
        return "break"

    def manual_copy(self, event=None):
        try:
            selection = self._textbox.get("sel.first", "sel.last")
            if selection:
                self.clipboard_clear()
                self.clipboard_append(selection)
        except Exception:
            pass
        return "break"

    def manual_select_all(self, event=None):
        self._textbox.tag_add("sel", "1.0", "end")
        return "break"

    def do_popup(self, event):
        m = tk.Menu(self, tearoff=0)
        m.add_command(label="Копировать", command=self.manual_copy)
        m.add_command(label="Вставить", command=self.manual_paste)
        m.add_command(label="Выделить всё", command=self.manual_select_all)
        m.tk_popup(event.x_root, event.y_root)

class VotingApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Decide DLT - Система коллективных решений")
        self.geometry("1100x700")

        self.engine = MVPEngine()
        self.current_user = None

        # Grid configuration
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # Sidebar
        self.sidebar_frame = ctk.CTkFrame(self, width=220, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, sticky="nsew")
        self.sidebar_frame.grid_rowconfigure(7, weight=1) # Spacer row

        self.logo_label = ctk.CTkLabel(self.sidebar_frame, text="Decide DLT", font=ctk.CTkFont(size=22, weight="bold"))
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 5))
        
        self.desc_label = ctk.CTkLabel(self.sidebar_frame, text="Прозрачные голосования\nс доверием", font=ctk.CTkFont(size=10), text_color="gray")
        self.desc_label.grid(row=1, column=0, padx=10, pady=(0, 20))

        self.user_label = ctk.CTkLabel(self.sidebar_frame, text="Пользователь: Не вошел", font=ctk.CTkFont(size=14))
        self.user_label.grid(row=2, column=0, padx=20, pady=(10, 5))

        self.profile_btn = ctk.CTkButton(self.sidebar_frame, text="Личный кабинет", command=self.show_profile, height=32, fg_color="#3b3b3b")
        self.profile_btn.grid(row=3, column=0, padx=20, pady=5)

        self.dashboard_btn = ctk.CTkButton(self.sidebar_frame, text="Голосования", command=self.show_dashboard, height=32)
        self.dashboard_btn.grid(row=4, column=0, padx=20, pady=5)

        self.blockchain_btn = ctk.CTkButton(self.sidebar_frame, text="Blockchain Explorer", command=self.show_blockchain, height=32)
        self.blockchain_btn.grid(row=5, column=0, padx=20, pady=5)

        self.peer_btn = ctk.CTkButton(self.sidebar_frame, text="Добавить Пир (IP)", command=self.show_add_peer, height=32)
        self.peer_btn.grid(row=6, column=0, padx=20, pady=5)

        self.logout_btn = ctk.CTkButton(self.sidebar_frame, text="Выход", command=self.logout, height=32, fg_color="#8B0000")
        self.logout_btn.grid(row=8, column=0, padx=20, pady=5)

        self.status_label = ctk.CTkLabel(self.sidebar_frame, text="Сеть: Ожидание пиров", text_color="orange", font=ctk.CTkFont(size=12))
        self.status_label.grid(row=9, column=0, padx=20, pady=20)

        # Main content area
        self.main_frame = ctk.CTkFrame(self, corner_radius=0, fg_color="transparent")
        self.main_frame.grid(row=0, column=1, sticky="nsew", padx=20, pady=20)
        self.main_frame.grid_columnconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(0, weight=1)

        # Dashboard
        self.show_login()
        
        # Periodic refresh
        self.periodic_refresh()

    def periodic_refresh(self):
        # Update dashboard if we are on it
        if hasattr(self, 'meetings_list') and self.meetings_list.winfo_exists():
            # Only auto-refresh if search is empty to avoid jumpy UI during typing
            # AND only if we are actually on the dashboard tab
            if not self.search_entry.get():
                self.refresh_meetings()
            
        # Update status label with peer count
        peer_count = len(self.engine.peers)
        if peer_count > 0:
            self.status_label.configure(text=f"Сеть: {peer_count} пиров", text_color="green")
        else:
            self.status_label.configure(text="Сеть: Ожидание пиров", text_color="orange")
            
        self.after(10000, self.periodic_refresh) # Every 10 seconds

    def show_login(self):
        self.clear_main_frame()
        login_frame = ctk.CTkFrame(self.main_frame, width=400, height=450)
        login_frame.place(relx=0.5, rely=0.5, anchor="center")

        # Welcome text
        ctk.CTkLabel(login_frame, text="Decide DLT", font=ctk.CTkFont(size=40, weight="bold")).pack(pady=(0, 10))
        ctk.CTkLabel(login_frame, text="Прозрачные голосования с персональным доверием", font=ctk.CTkFont(size=14)).pack(pady=(0, 30))
        self.username_entry = CustomEntry(login_frame, placeholder_text="Имя пользователя", width=300)
        self.username_entry.pack(pady=10)
        
        self.password_entry = CustomEntry(login_frame, placeholder_text="Пароль", width=300, show="*")
        self.password_entry.pack(pady=10)
        
        btn_frame = ctk.CTkFrame(login_frame, fg_color="transparent")
        btn_frame.pack(pady=20)
        
        ctk.CTkButton(btn_frame, text="Войти", command=self.login, width=140).pack(side="left", padx=10)
        ctk.CTkButton(btn_frame, text="Регистрация", command=self.register, width=140, fg_color="gray").pack(side="left", padx=10)

    def login(self):
        username = self.username_entry.get()
        password = self.password_entry.get()
        if username:
            success, result = self.engine.login_user(username, password)
            if success:
                self.current_user = username
                self.user_label.configure(text=f"Пользователь: {username}")
                self.show_dashboard()
            else:
                self.show_alert("Ошибка входа", result)

    def register(self):
        username = self.username_entry.get()
        password = self.password_entry.get()
        if username:
            success, result = self.engine.register_user(username, password)
            if success:
                self.show_alert("Успех", "Пользователь успешно зарегистрирован!")
                self.login()
            else:
                self.show_alert("Ошибка регистрации", result)

    def clear_main_frame(self):
        for widget in self.main_frame.winfo_children():
            widget.destroy()

    def logout(self):
        self.current_user = None
        self.user_label.configure(text="Пользователь: Не вошел")
        self.show_login()

    def show_profile(self):
        self.clear_main_frame()
        user_info = self.engine.users[self.current_user]
        
        ctk.CTkLabel(self.main_frame, text="Личный кабинет", font=ctk.CTkFont(size=24, weight="bold")).pack(pady=(0, 20))
        
        info_frame = ctk.CTkFrame(self.main_frame)
        info_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(info_frame, text=f"Имя пользователя: {self.current_user}", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=20, pady=5)
        
        id_frame = ctk.CTkFrame(info_frame, fg_color="transparent")
        id_frame.pack(fill="x", padx=20, pady=5)
        ctk.CTkLabel(id_frame, text="Ваш DLT ID:", font=ctk.CTkFont(weight="bold")).pack(side="left")
        id_entry = CustomEntry(id_frame, width=400, font=ctk.CTkFont(family="Courier", size=12))
        id_entry.insert(0, user_info['id'])
        id_entry.configure(state="readonly")
        id_entry.pack(side="left", padx=10)

        # Tabs for history
        tabview = ctk.CTkTabview(self.main_frame)
        tabview.pack(fill="both", expand=True, padx=20, pady=10)
        
        tab_votes = tabview.add("История голосов")
        tab_created = tabview.add("Созданные голосования")

        # History of votes
        votes_search_frame = ctk.CTkFrame(tab_votes, fg_color="transparent")
        votes_search_frame.pack(fill="x", pady=5)
        v_search = CustomEntry(votes_search_frame, placeholder_text="Поиск в истории голосов...")
        v_search.pack(side="left", fill="x", expand=True, padx=5)
        
        votes_list = ctk.CTkScrollableFrame(tab_votes)
        votes_list.pack(fill="both", expand=True, pady=5)

        def refresh_profile_votes(term=""):
            for w in votes_list.winfo_children(): w.destroy()
            voted = user_info.get("voted_meetings", [])
            for v in reversed(voted):
                m_id = v.get('meeting_id', '')
                tx_id = v.get('transaction_id', 'N/A')
                if term.lower() in v['title'].lower() or term.lower() in m_id.lower() or term.lower() in tx_id.lower():
                    vf = ctk.CTkFrame(votes_list)
                    vf.pack(fill="x", pady=2, padx=5)
                    
                    info_f = ctk.CTkFrame(vf, fg_color="transparent")
                    info_f.pack(side="left", fill="x", expand=True, padx=10)
                    
                    ctk.CTkLabel(info_f, text=v['title'], font=ctk.CTkFont(weight="bold")).pack(anchor="w")
                    
                    tx_f = ctk.CTkFrame(info_f, fg_color="transparent")
                    tx_f.pack(anchor="w")
                    ctk.CTkLabel(tx_f, text="TX ID:", font=ctk.CTkFont(size=10)).pack(side="left")
                    txe = CustomEntry(tx_f, width=250, font=ctk.CTkFont(size=9))
                    txe.insert(0, tx_id)
                    txe.configure(state="readonly")
                    txe.pack(side="left", padx=5)
                    
                    ctk.CTkLabel(vf, text=f"Голос: {v['option']}", text_color="#32CD32").pack(side="left", padx=20)
                    ctk.CTkLabel(vf, text=v['time'], font=ctk.CTkFont(size=10)).pack(side="right", padx=10)
        
        v_search.bind("<KeyRelease>", lambda e: refresh_profile_votes(v_search.get()))
        refresh_profile_votes()

        # History of created meetings
        created_search_frame = ctk.CTkFrame(tab_created, fg_color="transparent")
        created_search_frame.pack(fill="x", pady=5)
        c_search = CustomEntry(created_search_frame, placeholder_text="Поиск в созданных...")
        c_search.pack(side="left", fill="x", expand=True, padx=5)

        created_list = ctk.CTkScrollableFrame(tab_created)
        created_list.pack(fill="both", expand=True, pady=5)

        def refresh_profile_created(term=""):
            for w in created_list.winfo_children(): w.destroy()
            created_ids = user_info.get("created_meetings", [])
            for mid in reversed(created_ids):
                m = self.engine.meetings.get(mid)
                if m and (term.lower() in m.title.lower() or term.lower() in mid.lower()):
                    mf = ctk.CTkFrame(created_list)
                    mf.pack(fill="x", pady=2, padx=5)
                    ctk.CTkLabel(mf, text=m.title, font=ctk.CTkFont(weight="bold")).pack(side="left", padx=10)
                    
                    id_e = CustomEntry(mf, width=200, font=ctk.CTkFont(size=10))
                    id_e.insert(0, mid)
                    id_e.configure(state="readonly")
                    id_e.pack(side="left", padx=10)
                    
                    ctk.CTkLabel(mf, text=f"Статус: {m.status}").pack(side="right", padx=10)

        c_search.bind("<KeyRelease>", lambda e: refresh_profile_created(c_search.get()))
        refresh_profile_created()

    def show_dashboard(self):
        self.clear_main_frame()
        
        header_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 10))
        
        ctk.CTkLabel(header_frame, text="Голосования", font=ctk.CTkFont(size=24, weight="bold")).pack(side="left")
        
        # Action buttons frame
        actions_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        actions_frame.pack(side="right")

        ctk.CTkButton(actions_frame, text="+ Создать голосование", command=self.show_create_meeting, width=200).pack(side="left", padx=5)

        # Search frame
        search_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        search_frame.pack(fill="x", pady=10)
        
        self.search_entry = CustomEntry(search_frame, placeholder_text="Поиск по названию или ID...", width=400)
        self.search_entry.pack(side="left", padx=(0, 10))
        
        ctk.CTkButton(search_frame, text="Искать", width=100, command=self.refresh_meetings).pack(side="left", padx=5)
        ctk.CTkButton(search_frame, text="Сброс", width=100, fg_color="gray", command=lambda: [self.search_entry.delete(0, 'end'), self.refresh_meetings()]).pack(side="left", padx=5)

        # Scrollable list of meetings
        self.meetings_list = ctk.CTkScrollableFrame(self.main_frame)
        self.meetings_list.pack(fill="both", expand=True)
        
        self.refresh_meetings()

    def refresh_meetings(self, force=False):
        meetings = self.engine.get_meetings_list()
        
        # Simple check to see if we need to redraw
        current_meeting_ids = []
        if hasattr(self, 'meetings_list'):
            for widget in self.meetings_list.winfo_children():
                if hasattr(widget, 'meeting_id'):
                    current_meeting_ids.append(widget.meeting_id)
        
        new_meeting_ids = [m['meeting_id'] for m in meetings]
        
        search_term = self.search_entry.get().lower() if hasattr(self, 'search_entry') else ""
        if not force and not search_term and current_meeting_ids == new_meeting_ids:
            return

        for widget in self.meetings_list.winfo_children():
            widget.destroy()
            
        if not meetings:
            ctk.CTkLabel(self.meetings_list, text="Нет активных голосований").pack(pady=20)
            return
            
        for m in meetings:
            if search_term:
                found = search_term in m['title'].lower() or \
                        search_term in m['meeting_id'].lower() or \
                        search_term in m['initiator_id'].lower()
                if not found: continue
                
            m_frame = ctk.CTkFrame(self.meetings_list)
            m_frame.pack(fill="x", pady=5, padx=5)
            m_frame.meeting_id = m['meeting_id']
            
            # Left side: Title and ID
            info_f = ctk.CTkFrame(m_frame, fg_color="transparent")
            info_f.pack(side="left", fill="both", expand=True, padx=10, pady=5)
            
            # Title with wrapping
            title_l = ctk.CTkLabel(info_f, text=m['title'], font=ctk.CTkFont(size=16, weight="bold"), 
                                  wraplength=400, justify="left")
            title_l.pack(anchor="w")
            
            # ID Row
            id_f = ctk.CTkFrame(info_f, fg_color="transparent")
            id_f.pack(anchor="w", pady=(2, 0))
            ctk.CTkLabel(id_f, text="ID:", font=ctk.CTkFont(size=10)).pack(side="left")
            mid_e = CustomEntry(id_f, width=250, font=ctk.CTkFont(size=10))
            mid_e.insert(0, m['meeting_id'])
            mid_e.configure(state="readonly")
            mid_e.pack(side="left", padx=5)

            # Right side: Status and Button
            actions_f = ctk.CTkFrame(m_frame, fg_color="transparent")
            actions_f.pack(side="right", padx=10, pady=5)
            
            status_l = ctk.CTkLabel(actions_f, text=f"Статус: {m['status']}", font=ctk.CTkFont(size=12))
            status_l.pack(side="left", padx=10)
            
            ctk.CTkButton(actions_f, text="Участвовать", width=120, 
                          command=lambda mid=m['meeting_id']: self.show_vote_dialog(mid)).pack(side="left", padx=10)

    def show_join_by_id(self):
        dialog = ctk.CTkInputDialog(text="Введите уникальный ID голосования:", title="Вход в голосование")
        meeting_id = dialog.get_input()
        if meeting_id:
            if meeting_id in self.engine.meetings:
                self.show_vote_dialog(meeting_id)
            else:
                self.show_alert("Ошибка", f"Голосование {meeting_id} не найдено в вашей локальной базе.")

    def center_window(self, window, width, height):
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        window.geometry(f"{width}x{height}+{x}+{y}")

    def show_alert(self, title, message):
        alert = ctk.CTkToplevel(self)
        alert.title(title)
        self.center_window(alert, 300, 150)
        alert.grab_set()
        ctk.CTkLabel(alert, text=message, wraplength=250).pack(expand=True, padx=20, pady=20)
        ctk.CTkButton(alert, text="OK", command=alert.destroy, width=100).pack(pady=(0, 20))

    def show_create_meeting(self):
        dialog = ctk.CTkToplevel(self)
        dialog.title("Создание голосования")
        self.center_window(dialog, 600, 750)
        dialog.grab_set()

        ctk.CTkLabel(dialog, text="Новое голосование", font=ctk.CTkFont(size=20, weight="bold")).pack(pady=10)
        
        # Title and Agenda
        title_entry = CustomEntry(dialog, placeholder_text="Заголовок голосования", width=500)
        title_entry.pack(pady=5)
        
        agenda_entry = CustomEntry(dialog, placeholder_text="Вопрос для голосования", width=500)
        agenda_entry.pack(pady=5)

        options_entry = CustomEntry(dialog, placeholder_text="Варианты (через запятую: ДА, НЕТ)", width=500)
        options_entry.pack(pady=5)

        # Dates
        now = datetime.utcnow()
        tom = now + timedelta(days=1)

        def create_date_fields(parent, label, dt):
            f = ctk.CTkFrame(parent, fg_color="transparent")
            f.pack(pady=2)
            ctk.CTkLabel(f, text=label, width=80).pack(side="left")
            
            d = CustomEntry(f, width=40); d.insert(0, dt.strftime("%d")); d.pack(side="left", padx=2)
            ctk.CTkLabel(f, text=".").pack(side="left")
            m = CustomEntry(f, width=40); m.insert(0, dt.strftime("%m")); m.pack(side="left", padx=2)
            ctk.CTkLabel(f, text=".").pack(side="left")
            y = CustomEntry(f, width=60); y.insert(0, dt.strftime("%Y")); y.pack(side="left", padx=2)
            
            ctk.CTkLabel(f, text="  ").pack(side="left")
            
            hh = CustomEntry(f, width=40); hh.insert(0, dt.strftime("%H")); hh.pack(side="left", padx=2)
            ctk.CTkLabel(f, text=":").pack(side="left")
            mm = CustomEntry(f, width=40); mm.insert(0, dt.strftime("%M")); mm.pack(side="left", padx=2)
            
            return (d, m, y, hh, mm)

        start_fields = create_date_fields(dialog, "Начало:", now)
        end_fields = create_date_fields(dialog, "Конец:", tom)

        # Helper to get ISO string from fields
        def get_iso(fields):
            d, m, y, hh, mm = [f.get() for f in fields]
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}T{hh.zfill(2)}:{mm.zfill(2)}:00Z"

        # Allowed Voter IDs
        ctk.CTkLabel(dialog, text="Разрешенные ID участников (через запятую, пусто = все):", font=ctk.CTkFont(size=12)).pack(pady=(10, 0))
        voters_entry = CustomEntry(dialog, placeholder_text="ID1, ID2...", width=500)
        voters_entry.pack(pady=5)

        # Validator Selection
        ctk.CTkLabel(dialog, text="Выберите валидаторов для подтверждения создания:", font=ctk.CTkFont(weight="bold")).pack(pady=(10, 5))
        val_frame = ctk.CTkScrollableFrame(dialog, height=150, width=480)
        val_frame.pack(pady=5)
        
        val_checks = []
        for v in self.engine.validators:
            cb = ctk.CTkCheckBox(val_frame, text=f"{v.name} ({v.validator_id[:8]}...)")
            cb.pack(pady=2, anchor="w")
            cb.select()
            val_checks.append((cb, v.validator_id))

        def submit():
            title = title_entry.get()
            agenda_text = agenda_entry.get()
            opts = [o.strip() for o in options_entry.get().split(",") if o.strip()]
            try:
                start_t = get_iso(start_fields)
                end_t = get_iso(end_fields)
            except Exception as e:
                self.show_alert("Ошибка", f"Некорректный формат даты/времени: {e}")
                return
                
            voters = [v.strip() for v in voters_entry.get().split(",") if v.strip()]
            sel_vals = [vid for cb, vid in val_checks if cb.get()]

            if not (title and agenda_text and opts and sel_vals):
                self.show_alert("Ошибка", "Заполните все поля и выберите валидаторов!")
                return

            # Disable button to prevent double click
            submit_btn.configure(state="disabled")
            
            agenda = [{"item_number": 1, "description": agenda_text, "options": opts}]
            try:
                success, result = self.engine.create_meeting(self.current_user, title, agenda, start_t, end_t, voters, sel_vals)
                
                if success:
                    self.show_alert("Успех", f"Голосование создано!\nID: {result}")
                    dialog.destroy()
                    self.show_dashboard()
                else:
                    self.show_alert("Ошибка", result)
                    submit_btn.configure(state="normal")
            except Exception as e:
                self.show_alert("Критическая ошибка", str(e))
                submit_btn.configure(state="normal")

        submit_btn = ctk.CTkButton(dialog, text="Опубликовать в блокчейн", command=submit)
        submit_btn.pack(pady=20)

    def show_vote_dialog(self, meeting_id):
        m = self.engine.meetings[meeting_id]
        dialog = ctk.CTkToplevel(self)
        dialog.title(f"Голосование: {m.title}")
        self.center_window(dialog, 600, 500)
        dialog.grab_set()

        ctk.CTkLabel(dialog, text=m.title, font=ctk.CTkFont(size=20, weight="bold")).pack(pady=10)
        
        agenda_item = m.agenda[0]
        ctk.CTkLabel(dialog, text=f"Вопрос: {agenda_item['description']}", font=ctk.CTkFont(size=16)).pack(pady=10)

        vote_var = ctk.StringVar(value=agenda_item['options'][0])
        for opt in agenda_item['options']:
            ctk.CTkRadioButton(dialog, text=opt, variable=vote_var, value=opt).pack(pady=5)

        ctk.CTkLabel(dialog, text="Настройка DLT Валидаторов", font=ctk.CTkFont(weight="bold")).pack(pady=(20, 5))
        
        # Select validators
        val_ids = [v.validator_id for v in self.engine.validators]
        val_names = [v.name for v in self.engine.validators]
        
        val_checks = []
        for v in self.engine.validators:
            cb = ctk.CTkCheckBox(dialog, text=v.name)
            cb.pack(pady=2)
            cb.select() # select all by default
            val_checks.append((cb, v.validator_id))

        def submit_vote():
            selected_ids = [vid for cb, vid in val_checks if cb.get()]
            if not selected_ids:
                self.show_alert("Ошибка", "Выберите хотя бы одного валидатора!")
                return
            
            # Disable button to prevent double vote
            submit_btn.configure(state="disabled")
            
            try:
                success, msg = self.engine.cast_vote(self.current_user, meeting_id, 1, vote_var.get(), selected_ids, len(selected_ids)//2 + 1)
                
                if success:
                    self.show_alert("Успех", "Ваш голос принят и ожидает включения в блок!")
                    dialog.destroy()
                    self.show_dashboard()
                else:
                    self.show_alert("Ошибка", f"Голос не принят: {msg}")
                    submit_btn.configure(state="normal")
            except Exception as e:
                self.show_alert("Критическая ошибка", str(e))
                submit_btn.configure(state="normal")

        submit_btn = ctk.CTkButton(dialog, text="Проголосовать", command=submit_vote)
        submit_btn.pack(pady=20)

    def show_add_peer(self):
        dialog = ctk.CTkInputDialog(text="Введите IP адрес другого участника:", title="Добавить Пир")
        ip = dialog.get_input()
        if ip:
            self.engine.add_peer(ip)
            ctk.CTkLabel(self.sidebar_frame, text=f"Пир: {ip}", font=ctk.CTkFont(size=10)).grid(row=6, column=0)

    def show_blockchain(self):
        self.clear_main_frame()
        
        header_f = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        header_f.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(header_f, text="Blockchain Explorer", font=ctk.CTkFont(size=24, weight="bold")).pack(side="left")
        
        # Explorer Search
        self.explorer_search = CustomEntry(header_f, placeholder_text="Поиск по Hash / TX ID / Meeting ID / Voter ID...", width=400)
        self.explorer_search.pack(side="right", padx=10)
        self.explorer_search.bind("<KeyRelease>", lambda e: self.refresh_explorer())
        
        self.explorer_list = ctk.CTkScrollableFrame(self.main_frame)
        self.explorer_list.pack(fill="both", expand=True)
        
        self.refresh_explorer()

    def refresh_explorer(self):
        for widget in self.explorer_list.winfo_children():
            widget.destroy()
            
        search_term = self.explorer_search.get().strip().lower() if hasattr(self, 'explorer_search') else ""
        
        for block in reversed(self.engine.blockchain):
            # Enhanced Search logic for explorer
            match = False
            if not search_term:
                match = True
            else:
                # Check block hash
                if search_term in block.hash.lower(): match = True
                # Check block index
                elif search_term == str(block.index): match = True
                # Check transactions
                else:
                    for tx in block.transactions:
                        # Check TX ID
                        if search_term in tx.get('transaction_id', '').lower(): 
                            match = True
                            break
                        # Check Voter ID (sender)
                        if search_term in tx.get('sender', '').lower():
                            match = True
                            break
                        # Check payload content
                        payload_str = json.dumps(tx.get('payload', {})).lower()
                        if search_term in payload_str:
                            match = True
                            break
            
            if not match: continue

            b_frame = ctk.CTkFrame(self.explorer_list)
            b_frame.pack(fill="x", pady=10, padx=10)
            
            header_color = "#1f538d" if block.index > 0 else "#2d2d2d"
            header = ctk.CTkFrame(b_frame, fg_color=header_color, height=40)
            header.pack(fill="x")
            
            ctk.CTkLabel(header, text=f"БЛОК #{block.index}", font=ctk.CTkFont(weight="bold")).pack(side="left", padx=10)
            
            h_entry = CustomEntry(header, width=450, font=ctk.CTkFont(family="Courier", size=10))
            h_entry.insert(0, block.hash)
            h_entry.configure(state="readonly")
            h_entry.pack(side="right", padx=10)
            
            # Use Textbox for copyable multi-line details
            details_box = CustomTextbox(b_frame, height=150, font=ctk.CTkFont(family="Courier", size=11))
            details_box.pack(fill="x", padx=10, pady=10)
            
            content = f"Timestamp: {block.timestamp}\n"
            content += f"Prev Hash: {block.prev_hash}\n"
            content += f"Proposer:  {block.proposer}\n"
            content += f"Merkle:    {block.merkle_root}\n"
            content += f"Transactions ({len(block.transactions)}):\n"
            
            for tx in block.transactions:
                content += f"  - [{tx['type'].upper()}] TX: {tx['transaction_id']}\n"
                if tx['type'] == 'vote':
                    content += f"    Meeting ID: {tx['payload'].get('meeting_id', 'N/A')}\n"
                    content += f"    Voter: {tx['payload'].get('voter_id', 'N/A')} -> Option: {tx['payload'].get('vote_option', 'N/A')}\n"
                elif tx['type'] == 'init_meeting':
                    content += f"    Meeting ID: {tx['payload'].get('meeting_id', 'N/A')}\n"
                    content += f"    Title: {tx['payload'].get('title', 'N/A')}\n"
            
            details_box.insert("0.0", content)
            details_box.configure(state="disabled") # Keep it selectable but not editable

if __name__ == "__main__":
    app = VotingApp()
    if os.path.exists("icon.ico"):
        app.iconbitmap("icon.ico")
    app.mainloop()
