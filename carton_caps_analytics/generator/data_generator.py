"""
Carton Caps Data Generator

Generates synthetic data for the Carton Caps analytics pipeline challenge.

Outputs:
- schools.csv
- users.csv
- products.csv
- referrals.csv
- purchases.csv
- events.csv
- carton_caps_generated.db (SQLite copy of all generated tables)

Usage:
    python data_generator.py
"""

from __future__ import annotations

import csv
import os
import random
import sqlite3
import string
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class CartonCapsDataGenerator:
    """Generate synthetic data for Carton Caps analytics pipeline."""

    def __init__(self, seed: int = 42, output_dir: str = "./data"):
        random.seed(seed)
        self.output_dir = output_dir

        # Analytics window (where referrals/purchases/events happen)
        self.start_date = datetime(2024, 1, 1, 0, 0, 0)
        self.end_date = datetime(2024, 6, 30, 23, 59, 59)

        os.makedirs(self.output_dir, exist_ok=True)

        # Generated datasets
        self.schools: List[Dict] = []
        self.users: List[Dict] = []
        self.products: List[Dict] = []
        self.referrals: List[Dict] = []
        self.purchases: List[Dict] = []
        self.events: List[Dict] = []

    # -------------------------
    # Dimension generators
    # -------------------------

    def generate_schools(self, n: int = 50) -> List[Dict]:
        school_types = ["Elementary", "Middle", "High"]
        school_names = [
            "Sunnydale", "Springfield", "Hawkins", "Hill Valley", "Twin Peaks",
            "Rivendell", "Waterdeep", "Neverwinter", "Baldur", "Ravenloft",
            "Ooo", "Candy Kingdom", "Fire Kingdom", "Cloud Kingdom",
            "Arrakis", "Caladan", "Kaitain", "Ix",
            "Fairview", "Georgetown", "Riverside", "Madison",
        ]

        schools: List[Dict] = []
        used = set()

        for i in range(n):
            while True:
                name = random.choice(school_names)
                school_type = random.choice(school_types)
                key = f"{name} {school_type}"
                if key not in used:
                    used.add(key)
                    break

            created_at = self.start_date - timedelta(days=random.randint(365, 1825))
            schools.append(
                {
                    "school_id": i + 1,
                    "name": f"{name} {school_type} School",
                    "address": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'School'])} St",
                    "city": random.choice(["Springfield", "Riverside", "Madison", "Georgetown", "Fairview"]),
                    "state": random.choice(["CA", "TX", "NY", "FL", "IL"]),
                    "zip_code": f"{random.randint(10000, 99999)}",
                    "created_at": created_at,
                }
            )

        self.schools = schools
        return schools

    def generate_users(self, n: int = 1000) -> List[Dict]:
        """
        Generate baseline users.

        Adds a few extra fields for realism (not required by the prompt) that will help
        downstream product/trust dashboards:
        - is_verified
        - device_id
        - marketing_channel
        """
        if not self.schools:
            raise ValueError("Schools must be generated before users")

        first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
            "Michael", "Linda", "William", "Elizabeth", "David", "Barbara",
            "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
        ]
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
            "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
        ]

        users: List[Dict] = []

        # Some users exist prior to analytics window; others join during it
        existing_fraction = 0.65

        for i in range(n):
            first = random.choice(first_names)
            last = random.choice(last_names)

            if random.random() < existing_fraction:
                created_at = self.start_date - timedelta(days=random.randint(1, 365))
            else:
                created_at = self._random_datetime(self.start_date, self.end_date)

            user_type = random.choices(["parent", "teacher", "supporter"], weights=[0.7, 0.2, 0.1])[0]

            users.append(
                {
                    "user_id": i + 1,
                    "first_name": first,
                    "last_name": last,
                    "email": f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@email.com",
                    "school_id": random.choice(self.schools)["school_id"],
                    "created_at": created_at,
                    "user_type": user_type,
                    "is_verified": 1 if random.random() < 0.94 else 0,
                    "device_id": self._random_device_id(),
                    "marketing_channel": random.choices(
                        ["organic", "paid_social", "paid_search", "influencer", "partner", "school_campaign"],
                        weights=[0.35, 0.18, 0.18, 0.06, 0.08, 0.15],
                    )[0],
                }
            )

        self.users = users
        return users

    def generate_products(self, n: int = 100) -> List[Dict]:
        categories = {
            "Breakfast": ["Cereal", "Oatmeal", "Granola Bars", "Yogurt", "Waffles"],
            "Snacks": ["Chips", "Crackers", "Cookies", "Pretzels", "Popcorn"],
            "Beverages": ["Juice", "Water", "Sports Drink", "Tea", "Coffee"],
            "Dairy": ["Milk", "Cheese", "Butter", "Ice Cream", "Cream Cheese"],
            "Pantry": ["Pasta", "Rice", "Soup", "Sauce", "Oil"],
        }

        brands = [
            "Wonka", "Duff", "Buzz Cola", "Krusty", "Lard Lad",
            "Soylent", "Brawndo", "Slurm", "Bachelor Chow",
            "Initech", "Umbrella", "Tyrell", "Cyberdyne",
            "Atreides", "Harkonnen", "CHOAM",
        ]

        products: List[Dict] = []
        product_id = 1

        target_per_item = max(1, n // 25)

        for category, items in categories.items():
            for item in items:
                for _ in range(target_per_item):
                    base_price = round(random.uniform(1.99, 9.99), 2)
                    products.append(
                        {
                            "product_id": product_id,
                            "name": f"{random.choice(brands)} {item}",
                            "category": category,
                            "price": base_price,
                            "points_per_dollar": random.choice([1, 2, 3, 5]),
                            "created_at": self.start_date - timedelta(days=random.randint(30, 365)),
                        }
                    )
                    product_id += 1

        self.products = products[:n]
        return self.products

    # -------------------------
    # Fact generators
    # -------------------------

    def generate_referrals(self, n: int = 1000) -> List[Dict]:
        """
        Generate referral network data with realistic-ish funnel:
        sent -> clicked -> converted

        "Converted" represents a reward-eligible referral outcome in analytics terms.
        We also create the referred user row for converted referrals.
        """
        if not self.users:
            raise ValueError("Users must be generated before referrals")
        if not self.products:
            raise ValueError("Products must be generated before referrals")

        existing_emails = {u["email"].lower() for u in self.users}
        existing_device_ids = {u.get("device_id") for u in self.users if u.get("device_id")}
        next_user_id = max(u["user_id"] for u in self.users) + 1

        # Prefer verified users as referrers
        referrer_population = [u["user_id"] for u in self.users]
        referrer_weights = []
        for u in self.users:
            if not u.get("is_verified", 1):
                referrer_weights.append(0.05)  # very low
                continue
            base = random.paretovariate(2.5)  # heavy tail super-referrers
            type_mult = 1.15 if u.get("user_type") == "parent" else 1.0
            channel_mult = 1.2 if u.get("marketing_channel") in ("school_campaign", "partner") else 1.0
            referrer_weights.append(base * type_mult * channel_mult)

        total_w = sum(referrer_weights) or 1.0
        referrer_probs = [w / total_w for w in referrer_weights]

        # Funnel parameters (tunable)
        click_rate = 0.55
        conversion_given_click = 0.32  # overall ~ 17-18%

        referrals: List[Dict] = []
        assigned_referred_emails: set[str] = set()

        referral_id = 1

        for _ in range(n):
            referrer_user_id = random.choices(referrer_population, weights=referrer_probs, k=1)[0]
            referrer = self.users[referrer_user_id - 1]

            sent_at = self._random_datetime(self.start_date, self.end_date)

            referred_email = self._random_new_email(existing_emails | assigned_referred_emails)
            referral_code = self._make_referral_code(referrer_user_id)

            clicked = random.random() < click_rate
            converted = clicked and (random.random() < conversion_given_click)

            status = "sent"
            converted_at: Optional[datetime] = None
            referred_user_id: Optional[int] = None

            if clicked:
                status = "clicked"

            if converted:
                # Conversion time (proxy for reward-eligible time), mostly within 48h
                signup_delay_hours = random.choices(
                    [1, 2, 6, 12, 24, 36, 48, 72],
                    weights=[0.10, 0.12, 0.18, 0.16, 0.18, 0.14, 0.10, 0.02],
                )[0]
                converted_at = sent_at + timedelta(hours=signup_delay_hours)
                if converted_at > self.end_date:
                    converted = False
                    converted_at = None

            # Basic self-referral / abuse attempt injection (very small)
            if converted and (referred_email.lower() == referrer["email"].lower() or random.random() < 0.01):
                converted = False
                converted_at = None
                status = "clicked"

            if converted and converted_at:
                # Create referred user
                referred_user_id = next_user_id
                next_user_id += 1

                # Social locality: likely same school, some drift
                school_id = referrer["school_id"] if random.random() < 0.75 else random.choice(self.schools)["school_id"]

                # Device id; allow rare collisions to simulate suspicious behavior (but still create user)
                device_id = self._random_device_id()
                if random.random() < 0.01 and existing_device_ids:
                    device_id = random.choice(list(existing_device_ids))
                existing_device_ids.add(device_id)

                first = random.choice(["Alex", "Jordan", "Taylor", "Casey", "Riley", "Morgan", "Avery", "Sam"])
                last = random.choice(["Lee", "Nguyen", "Patel", "Kim", "Garcia", "Brown", "Davis", "Wilson"])

                new_user = {
                    "user_id": referred_user_id,
                    "first_name": first,
                    "last_name": last,
                    "email": referred_email,
                    "school_id": school_id,
                    "created_at": converted_at,
                    "user_type": random.choices(["parent", "teacher", "supporter"], weights=[0.72, 0.18, 0.10])[0],
                    "is_verified": 1 if random.random() < 0.92 else 0,
                    "device_id": device_id,
                    "marketing_channel": "referral",
                }

                self.users.append(new_user)
                existing_emails.add(referred_email.lower())

                status = "converted"

            assigned_referred_emails.add(referred_email.lower())

            referrals.append(
                {
                    "referral_id": referral_id,
                    "referrer_user_id": referrer_user_id,
                    "referred_email": referred_email,
                    "referred_user_id": referred_user_id,
                    "referral_code": referral_code,
                    "sent_at": sent_at,
                    "converted_at": converted_at,
                    "status": status,
                }
            )
            referral_id += 1

        self.referrals = referrals
        return referrals

    def generate_purchases(self, n: int = 10000) -> List[Dict]:
        """
        Generate purchase records with realistic patterns.

        Output fields (exact):
            - purchase_id
            - user_id
            - product_id
            - quantity
            - price_paid
            - points_earned
            - purchased_at
            - day_of_week
            - hour_of_day
        """
        if not self.users or not self.products:
            raise ValueError("Users and products must be generated before purchases")

        product_by_id = {p["product_id"]: p for p in self.products}
        products_by_category: Dict[str, List[int]] = {}
        for p in self.products:
            products_by_category.setdefault(p["category"], []).append(p["product_id"])

        # Heavy-tailed user activity
        user_ids = [u["user_id"] for u in self.users]
        user_weights = []
        for u in self.users:
            base = random.paretovariate(2.0)
            type_mult = 1.25 if u.get("user_type") in ("parent", "teacher") else 0.9
            verified_mult = 1.15 if u.get("is_verified", 1) else 0.6
            channel_mult = 1.15 if u.get("marketing_channel") == "school_campaign" else 1.0
            user_weights.append(base * type_mult * verified_mult * channel_mult)

        total_w = sum(user_weights) or 1.0
        user_probs = [w / total_w for w in user_weights]

        def pick_category(user_type: str) -> str:
            if user_type == "parent":
                cats = ["Breakfast", "Dairy", "Pantry", "Snacks", "Beverages"]
                weights = [0.28, 0.22, 0.20, 0.18, 0.12]
            elif user_type == "teacher":
                cats = ["Snacks", "Beverages", "Breakfast", "Pantry", "Dairy"]
                weights = [0.26, 0.22, 0.18, 0.18, 0.16]
            else:
                cats = ["Snacks", "Beverages", "Pantry", "Breakfast", "Dairy"]
                weights = [0.30, 0.28, 0.18, 0.14, 0.10]
            return random.choices(cats, weights=weights, k=1)[0]

        # Temporal weighting
        weekday_weights = [1.0, 0.95, 0.95, 1.0, 1.05, 1.25, 1.20]  # Mon..Sun
        hour_weights = []
        for h in range(24):
            if 6 <= h <= 9:
                hour_weights.append(1.2)
            elif 10 <= h <= 15:
                hour_weights.append(1.0)
            elif 16 <= h <= 20:
                hour_weights.append(1.35)
            elif 21 <= h <= 22:
                hour_weights.append(0.9)
            else:
                hour_weights.append(0.25)
        hour_total = sum(hour_weights)
        hour_probs = [w / hour_total for w in hour_weights]

        # Candidate dates in window
        days = (self.end_date.date() - self.start_date.date()).days + 1
        date_candidates = [self.start_date.date() + timedelta(days=i) for i in range(days)]
        dow_total = sum(weekday_weights)
        dow_probs = [w / dow_total for w in weekday_weights]

        def pick_date() -> datetime:
            dow = random.choices(range(7), weights=dow_probs, k=1)[0]
            matches = [d for d in date_candidates if d.weekday() == dow]
            chosen = random.choice(matches)
            return datetime(chosen.year, chosen.month, chosen.day, 0, 0, 0)

        purchases: List[Dict] = []
        purchase_id = 1

        while len(purchases) < n:
            user_id = random.choices(user_ids, weights=user_probs, k=1)[0]
            user = self.users[user_id - 1]
            utype = user.get("user_type", "parent")

            base_day = pick_date()
            hour = random.choices(range(24), weights=hour_probs, k=1)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            purchased_at = base_day.replace(hour=hour, minute=minute, second=second)

            category = pick_category(utype)
            if category not in products_by_category:
                category = random.choice(list(products_by_category.keys()))
            product_id = random.choice(products_by_category[category])
            product = product_by_id[product_id]

            quantity = random.choices([1, 2, 3, 4, 5, 6], weights=[0.70, 0.18, 0.07, 0.03, 0.015, 0.005])[0]

            promo = random.random() < 0.12
            discount = random.uniform(0.05, 0.35) if promo else 0.0

            line_total = product["price"] * quantity * (1 - discount)
            noise = random.uniform(-0.03, 0.03)
            price_paid = round(max(0.5, line_total * (1 + noise)), 2)

            points_earned = int(round(price_paid * product["points_per_dollar"]))

            purchases.append(
                {
                    "purchase_id": purchase_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "price_paid": price_paid,
                    "points_earned": points_earned,
                    "purchased_at": purchased_at,
                    "day_of_week": purchased_at.strftime("%A"),
                    "hour_of_day": purchased_at.hour,
                }
            )
            purchase_id += 1

        # Inject a qualifying purchase for converted referred users shortly after conversion
        # (this creates realistic "qualifying action" timing for referral rules analytics)
        converted = [r for r in self.referrals if r.get("status") == "converted" and r.get("referred_user_id") and r.get("converted_at")]
        if converted:
            extra_purchase_id = purchase_id
            for r in converted:
                if random.random() < 0.85:
                    referred_user_id = r["referred_user_id"]
                    base = r["converted_at"]
                    qp_time = base + timedelta(days=random.randint(0, 6), hours=random.randint(8, 20))
                    if qp_time > self.end_date:
                        continue

                    product = random.choice(self.products)
                    qty = random.choices([1, 2, 3], weights=[0.72, 0.22, 0.06])[0]
                    price_paid = round(product["price"] * qty * (1 - random.uniform(0.0, 0.18)), 2)
                    points_earned = int(round(price_paid * product["points_per_dollar"]))

                    purchases.append(
                        {
                            "purchase_id": extra_purchase_id,
                            "user_id": referred_user_id,
                            "product_id": product["product_id"],
                            "quantity": qty,
                            "price_paid": price_paid,
                            "points_earned": points_earned,
                            "purchased_at": qp_time,
                            "day_of_week": qp_time.strftime("%A"),
                            "hour_of_day": qp_time.hour,
                        }
                    )
                    extra_purchase_id += 1

        self.purchases = purchases
        return purchases

    def generate_events(self) -> List[Dict]:
        """
        Generate product + referral lifecycle events.

        This table enables:
        - Referral rules modeling (install, referral_applied within 48h, onboarding, school_linked)
        - Product funnels (app_open -> receipt_scan_started -> receipt_scan_completed)
        - Finance (reward_awarded vs reward_redeemed timing)
        """
        if not self.users:
            raise ValueError("Users must exist before events")
        if not self.referrals:
            raise ValueError("Referrals must exist before events")
        if not self.purchases:
            raise ValueError("Purchases must exist before events")

        events: List[Dict] = []
        event_id = 1

        # USER BASELINE EVENTS
        for user in self.users:
            user_id = user["user_id"]
            created_at = user["created_at"]

            # Treat install == account creation for baseline users
            events.append(
                {
                    "event_id": event_id,
                    "user_id": user_id,
                    "event_type": "install",
                    "event_at": created_at,
                    "referral_id": None,
                    "metadata_json": "{}",
                }
            )
            event_id += 1

            # App opens (heavy tail)
            open_count = random.choices(
                [0, 1, 2, 3, 5, 8, 12],
                weights=[0.10, 0.25, 0.22, 0.18, 0.15, 0.07, 0.03],
            )[0]
            for _ in range(open_count):
                open_time = created_at + timedelta(
                    days=random.randint(0, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                if self.start_date <= open_time <= self.end_date:
                    events.append(
                        {
                            "event_id": event_id,
                            "user_id": user_id,
                            "event_type": "app_open",
                            "event_at": open_time,
                            "referral_id": None,
                            "metadata_json": "{}",
                        }
                    )
                    event_id += 1

        # REFERRAL LIFECYCLE EVENTS
        for referral in self.referrals:
            ref_id = referral["referral_id"]
            referrer_id = referral["referrer_user_id"]
            referred_user_id = referral.get("referred_user_id")

            # invite_sent by referrer
            events.append(
                {
                    "event_id": event_id,
                    "user_id": referrer_id,
                    "event_type": "invite_sent",
                    "event_at": referral["sent_at"],
                    "referral_id": ref_id,
                    "metadata_json": "{}",
                }
            )
            event_id += 1

            # clicked implies some engagement by the referred party; only if we have a referred_user_id
            # (In this generator, referred_user_id exists for converted, but not for clicked-only.)
            if referral["status"] in ("clicked", "converted"):
                if referred_user_id is not None:
                    # install for referred user slightly after sent
                    install_at = referral["sent_at"] + timedelta(hours=random.randint(1, 24))
                    events.append(
                        {
                            "event_id": event_id,
                            "user_id": referred_user_id,
                            "event_type": "install",
                            "event_at": install_at,
                            "referral_id": ref_id,
                            "metadata_json": '{"source":"referral"}',
                        }
                    )
                    event_id += 1

                    # referral_applied time — sometimes >48h to create ineligible examples
                    apply_delay_hours = random.choices(
                        [2, 6, 12, 24, 36, 48, 72],
                        weights=[0.20, 0.18, 0.16, 0.18, 0.14, 0.10, 0.04],
                    )[0]
                    applied_at = install_at + timedelta(hours=apply_delay_hours)
                    if applied_at <= self.end_date:
                        events.append(
                            {
                                "event_id": event_id,
                                "user_id": referred_user_id,
                                "event_type": "referral_applied",
                                "event_at": applied_at,
                                "referral_id": ref_id,
                                "metadata_json": "{}",
                            }
                        )
                        event_id += 1

            # converted events (onboarding, school_linked, reward award/redeem)
            if referral["status"] == "converted" and referral.get("converted_at") and referred_user_id is not None:
                converted_at = referral["converted_at"]

                onboarding_at = converted_at - timedelta(hours=random.randint(1, 12))
                if onboarding_at >= self.start_date:
                    events.append(
                        {
                            "event_id": event_id,
                            "user_id": referred_user_id,
                            "event_type": "onboarding_complete",
                            "event_at": onboarding_at,
                            "referral_id": ref_id,
                            "metadata_json": "{}",
                        }
                    )
                    event_id += 1

                school_linked_at = onboarding_at + timedelta(days=random.randint(0, 2))
                if school_linked_at <= self.end_date:
                    events.append(
                        {
                            "event_id": event_id,
                            "user_id": referred_user_id,
                            "event_type": "school_linked",
                            "event_at": school_linked_at,
                            "referral_id": ref_id,
                            "metadata_json": "{}",
                        }
                    )
                    event_id += 1

                # reward awarded to both parties
                for reward_user_id, reward_type in [
                    (referrer_id, "referrer_bonus"),
                    (referred_user_id, "referred_bonus"),
                ]:
                    events.append(
                        {
                            "event_id": event_id,
                            "user_id": reward_user_id,
                            "event_type": "reward_awarded",
                            "event_at": converted_at,
                            "referral_id": ref_id,
                            "metadata_json": f'{{"reward_type":"{reward_type}"}}',
                        }
                    )
                    event_id += 1

                    # reward redeemed later (not always)
                    if random.random() < 0.75:
                        redeemed_at = converted_at + timedelta(days=random.randint(1, 30))
                        if redeemed_at <= self.end_date:
                            events.append(
                                {
                                    "event_id": event_id,
                                    "user_id": reward_user_id,
                                    "event_type": "reward_redeemed",
                                    "event_at": redeemed_at,
                                    "referral_id": ref_id,
                                    "metadata_json": f'{{"reward_type":"{reward_type}"}}',
                                }
                            )
                            event_id += 1

        # RECEIPT SCAN / INCENTIVE EVENTS (from purchases)
        for purchase in self.purchases:
            user_id = purchase["user_id"]
            purchased_at = purchase["purchased_at"]

            scan_start = purchased_at - timedelta(minutes=random.randint(1, 5))
            if scan_start >= self.start_date:
                events.append(
                    {
                        "event_id": event_id,
                        "user_id": user_id,
                        "event_type": "receipt_scan_started",
                        "event_at": scan_start,
                        "referral_id": None,
                        "metadata_json": "{}",
                    }
                )
                event_id += 1

            # completion (some fail)
            if random.random() < 0.88:
                events.append(
                    {
                        "event_id": event_id,
                        "user_id": user_id,
                        "event_type": "receipt_scan_completed",
                        "event_at": purchased_at,
                        "referral_id": None,
                        "metadata_json": "{}",
                    }
                )
                event_id += 1

            # incentive viewed (optional)
            if random.random() < 0.35:
                viewed_at = purchased_at - timedelta(minutes=random.randint(5, 60))
                if viewed_at >= self.start_date:
                    events.append(
                        {
                            "event_id": event_id,
                            "user_id": user_id,
                            "event_type": "incentive_viewed",
                            "event_at": viewed_at,
                            "referral_id": None,
                            "metadata_json": "{}",
                        }
                    )
                    event_id += 1

        self.events = events
        return events

    # -------------------------
    # Persistence helpers
    # -------------------------

    def save_to_csv(self, data: List[Dict], filename: str) -> None:
        if not data:
            print(f"Warning: No data to save for {filename}")
            return

        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            writer.writeheader()
            writer.writerows(data)

        print(f"Saved {len(data)} records to {filepath}")

    def save_to_sqlite(self, db_path: Optional[str] = None) -> None:
        if db_path is None:
            db_path = os.path.join(self.output_dir, "carton_caps_generated.db")

        # Recreate DB each run for cleanliness
        if os.path.exists(db_path):
            os.remove(db_path)

        conn = sqlite3.connect(db_path)

        def create_and_insert(table_name: str, data: List[Dict]):
            if not data:
                return

            # Create table based on first record
            columns = []
            for key, value in data[0].items():
                if isinstance(value, int):
                    col_type = "INTEGER"
                elif isinstance(value, float):
                    col_type = "REAL"
                elif isinstance(value, datetime):
                    col_type = "TIMESTAMP"
                elif value is None:
                    col_type = "TEXT"
                else:
                    col_type = "TEXT"
                columns.append(f"{key} {col_type}")

            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
            conn.execute(create_sql)

            placeholders = ", ".join(["?" for _ in data[0].keys()])
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            for record in data:
                values = [record.get(key) for key in data[0].keys()]
                conn.execute(insert_sql, values)

        create_and_insert("schools", self.schools)
        create_and_insert("users", self.users)
        create_and_insert("products", self.products)
        create_and_insert("referrals", self.referrals)
        create_and_insert("purchases", self.purchases)
        create_and_insert("events", self.events)

        conn.commit()
        conn.close()
        print(f"Saved all data to {db_path}")

    def generate_all(
        self,
        n_schools: int = 50,
        n_users: int = 1000,
        n_products: int = 100,
        n_referrals: int = 1000,
        n_purchases: int = 10000,
    ) -> None:
        print("Generating Carton Caps data...")
        print("-" * 40)

        print(f"Generating {n_schools} schools...")
        self.generate_schools(n_schools)
        self.save_to_csv(self.schools, "schools.csv")

        print(f"Generating {n_users} users...")
        self.generate_users(n_users)
        self.save_to_csv(self.users, "users.csv")

        print(f"Generating {n_products} products...")
        self.generate_products(n_products)
        self.save_to_csv(self.products, "products.csv")

        # IMPORTANT: referrals before purchases so referred users exist for purchase generation
        print(f"Generating {n_referrals} referrals (may add new users)...")
        self.generate_referrals(n_referrals)
        self.save_to_csv(self.referrals, "referrals.csv")

        print(f"Generating {n_purchases} purchases...")
        self.generate_purchases(n_purchases)
        self.save_to_csv(self.purchases, "purchases.csv")

        print("Generating events (product + referral lifecycle)...")
        self.generate_events()
        self.save_to_csv(self.events, "events.csv")

        print("\nSaving to SQLite database...")
        self.save_to_sqlite()

        print("-" * 40)
        print("Data generation complete!")

    # -------------------------
    # Internal utilities
    # -------------------------

    @staticmethod
    def _random_datetime(start: datetime, end: datetime) -> datetime:
        """Uniform random datetime between start and end (inclusive)."""
        if end <= start:
            return start
        delta = end - start
        seconds = random.randint(0, int(delta.total_seconds()))
        return start + timedelta(seconds=seconds)

    @staticmethod
    def _random_device_id() -> str:
        return "dev_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=16))

    @staticmethod
    def _make_referral_code(user_id: int) -> str:
        suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
        return f"CC{user_id:05d}{suffix}"

    @staticmethod
    def _random_new_email(used: set[str]) -> str:
        domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "proton.me"]
        while True:
            handle = "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 10)))
            handle += str(random.randint(1, 9999))
            email = f"{handle}@{random.choice(domains)}"
            if email.lower() not in used:
                return email


if __name__ == "__main__":
    generator = CartonCapsDataGenerator(seed=42, output_dir="./data")

    generator.generate_all(
        n_schools=50,
        n_users=1000,
        n_products=100,
        n_referrals=1000,
        n_purchases=10000,
    )

    print("\nArtifacts written to ./data/")
    print(" - schools.csv, users.csv, products.csv, referrals.csv, purchases.csv, events.csv")
    print(" - carton_caps_generated.db")