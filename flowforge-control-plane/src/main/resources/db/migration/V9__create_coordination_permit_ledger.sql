CREATE TABLE coordination_resource (
    resource_key VARCHAR(300) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_coordination_resource_time CHECK (updated_at >= created_at)
);

CREATE TABLE coordination_permit (
    token UUID PRIMARY KEY,
    resource_key VARCHAR(300) NOT NULL REFERENCES coordination_resource(resource_key),
    holder_id VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    renewed_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ,
    CONSTRAINT ck_coordination_permit_status CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED')),
    CONSTRAINT ck_coordination_permit_time CHECK (
        renewed_at >= created_at AND expires_at > renewed_at
    ),
    CONSTRAINT ck_coordination_permit_release CHECK (
        (status = 'ACTIVE' AND released_at IS NULL)
        OR (status <> 'ACTIVE' AND released_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_coordination_permit_active_holder
    ON coordination_permit(resource_key, holder_id)
    WHERE status = 'ACTIVE';

CREATE INDEX ix_coordination_permit_active_expiry
    ON coordination_permit(resource_key, expires_at, token)
    WHERE status = 'ACTIVE';
