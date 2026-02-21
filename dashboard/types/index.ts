export interface ThreatDomain {
  domain: string;
  url: string;
  firstSeen: string;
  label: string;
  source: string;
  riskScore: number;
  riskLevel: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
  threatType: string;
  country?: string;
  registrar?: string;
  domainAgeDays?: number;
}

export interface DashboardStats {
  totalSamples: number;
  maliciousCount: number;
  benignCount: number;
  riskDistribution: Record<string, number>;
  threatTypeDistribution: Record<string, number>;
  sourceDistribution: Record<string, number>;
  averageRiskScore: number;
  highRiskCount: number;
  dateRange: {
    earliest: string | null;
    latest: string | null;
  };
  topCountries?: Record<string, number>;
  topRegistrars?: Record<string, number>;
}

export interface ModelMeta {
  created_utc: string;
  n_rows: number;
  n_pos: number;
  n_neg: number;
  metrics: {
    roc_auc: number;
    pr_auc: number;
    recall_at_1pct: number;
  };
  model_file: string;
}
