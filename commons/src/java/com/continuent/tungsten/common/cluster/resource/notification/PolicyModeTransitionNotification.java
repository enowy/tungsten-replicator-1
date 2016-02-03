package com.continuent.tungsten.common.cluster.resource.notification;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;

public class PolicyModeTransitionNotification
        extends ClusterResourceNotification
{
    private static final long serialVersionUID = 1L;
    private String prevMode;
    private String currentMode;
    private boolean locked;
    private boolean logged;
    
    public PolicyModeTransitionNotification(String clusterName, String memberName,
            String prevMode, String currentMode, boolean locked)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                null, ResourceType.POLICY_MANAGER, memberName, ResourceState.UNKNOWN,
                null);
       this.setPrevMode(prevMode);
       this.currentMode = currentMode;
       this.locked = locked;
       this.setLogged(false);
    }

    public String getPrevMode()
    {
        return prevMode;
    }

    public void setPrevMode(String prevMode)
    {
        this.prevMode = prevMode;
    }

    public String getCurrentMode()
    {
        return currentMode;
    }

    public void setCurrentMode(String currentMode)
    {
        this.currentMode = currentMode;
    }

    public boolean isLocked()
    {
        return locked;
    }

    public void setLocked(boolean locked)
    {
        this.locked = locked;
    }

    public boolean isLogged()
    {
        return logged;
    }

    public void setLogged(boolean logged)
    {
        this.logged = logged;
    }
    
    public String toString()
    {
        return String.format("POLICY MODE CHANGED BY '%s' TRANSITION %s => %s LOCKED=%s", memberName, prevMode, currentMode, locked);
    }
}
