export interface Campaign {
  id: string;
  name: string;
  status: string | number;
  sequences?: Sequence[];
  email_tag_list?: string[];
  timestamp_created?: string;
  timestamp_updated?: string;
  daily_limit?: number;
  campaign_schedule?: unknown;
  [key: string]: unknown;
}

export interface Sequence {
  steps: Step[];
}

export interface Step {
  type: string;
  delay: number;
  variants: Variant[];
}

export interface Variant {
  subject: string;
  body: string;
  v_disabled?: boolean;
}

export interface StepAnalytics {
  step: string | null;
  variant: string | null;
  sent: number;
  replies: number;
  unique_replies: number;
  replies_automatic: number;
  unique_replies_automatic: number;
  opportunities: number;
  unique_opportunities: number;
}

export interface CampaignAnalytics {
  leads_count: number;
  contacted_count: number;
  completed_count: number;
  bounced_count: number;
  unsubscribed_count: number;
}

export interface CampaignDailyAnalytics {
  date: string;
  sent: number;
  contacted: number;
  new_leads_contacted: number;
  opened: number;
  unique_opened: number;
  replies: number;
  unique_replies: number;
  replies_automatic: number;
  unique_replies_automatic: number;
  clicks: number;
  unique_clicks: number;
  opportunities: number;
  unique_opportunities: number;
}

export interface Account {
  email: string;
  status?: string;
  provider_code?: number;
  warmup_status?: string;
  daily_sent_count?: number;
  weekly_sent_count?: number;
  monthly_sent_count?: number;
  health_score?: number;
  [key: string]: unknown;
}

export interface Tag {
  id: string;
  label: string;
  organization_id: string;
}

export interface TagMapping {
  id: string;
  tag_id: string;
  resource_id: string;
  resource_type: number;
}
