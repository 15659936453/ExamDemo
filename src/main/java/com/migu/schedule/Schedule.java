package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {
    //已注册的服务节点信息
    Map<Integer,List<TaskInfo>> nodeMap = new HashMap<Integer,List<TaskInfo>>();
    //处于挂起状态的任务列表
    Map<Integer,Integer> pendingTasks = new HashMap<Integer,Integer>();
    public static final Integer DEFAULTVALUE = 0 ;
    public int init()
    {
        nodeMap.clear();
        pendingTasks.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId)
    {
        if(nodeId<=0)
        {
            return ReturnCodeKeys.E004;
        }
        if(nodeMap.containsKey(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        nodeMap.put(nodeId,null);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId)
    {
        if(nodeId<=0)
        {
            return ReturnCodeKeys.E004;
        }
        if(!nodeMap.containsKey(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        nodeMap.remove(nodeId);
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption)
    {
        if(taskId<=0)
        {
           return  ReturnCodeKeys.E009;
        }
        if(pendingTasks.containsKey(taskId))
        {
            return ReturnCodeKeys.E010;
        }
        pendingTasks.put(taskId,consumption);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId)
    {
        if(taskId<=0)
        {
            return  ReturnCodeKeys.E009;
        }
        if(pendingTasks.containsKey(taskId))
        {
            pendingTasks.remove(taskId);
            return ReturnCodeKeys.E011;
        }
        else if(checkNodeMap(taskId))
        {
            return ReturnCodeKeys.E011;
        }
        else
        {
            return ReturnCodeKeys.E012;
        }

    }


    public int scheduleTask(int threshold)
    {
        if(threshold<=0)
        {
            return ReturnCodeKeys.E002;
        }
        List<TaskInfo> taskInfos= getTaskInfoList(nodeMap,pendingTasks);

        return ReturnCodeKeys.E000;
    }


    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        if(DEFAULTVALUE==tasks.size() || tasks.isEmpty())
        {
            return ReturnCodeKeys.E016;
        }
        List<TaskInfo> taskInfos = new ArrayList<TaskInfo>(tasks.size());
        for(TaskInfo taskInfo:tasks)
        {
            if(pendingTasks.containsKey(taskInfo.getTaskId()))
            {
                taskInfo.setNodeId(-1);
            }
            taskInfos.add(taskInfo);
        }
        tasks.clear();
        taskSorter(taskInfos);
        return ReturnCodeKeys.E015;
    }

    /**
     * 检查挂起任务中是否包含待添加的任务
     * @param taskId
     * @return
     */
    private boolean checkNodeMap(int taskId)
    {
        if(nodeMap.size()<=0 || nodeMap.isEmpty())
        {
            return false;
        }
        for(Integer nodeId:nodeMap.keySet())
        {
            int nodeID=nodeId;
            List<TaskInfo> tasks = nodeMap.get(nodeID);
            if(null!=tasks)
            {
                for(int i=0;i<tasks.size();i++)
                {
                    TaskInfo taskInfo = tasks.get(i);
                    if(taskId==taskInfo.getTaskId())
                    {
                        tasks.remove(i);
                        nodeMap.remove(nodeID);
                        nodeMap.put(nodeID,tasks);
                        return true;
                    }
                }
            }

        }
        return false;
    }

    /**
     * 按照taskId升序排序
     * @param taskInfos
     */
    protected void taskSorter(List<TaskInfo> taskInfos)
    {
        Collections.sort(taskInfos, new Comparator<TaskInfo>()
        {
            public int compare(TaskInfo o1, TaskInfo o2)
            {
                return nodeIdSorterComparator(o1, o2);
            }
        });
    }

    protected int nodeIdSorterComparator(TaskInfo o1, TaskInfo o2)
    {
        int index1 = -1;
        if (o1.getNodeId() >0)
        {
            index1 = o1.getNodeId();
        }

        int index2 = -1;
        if (o2.getNodeId() >0)
        {
            index2 = o2.getNodeId();
        }

        return index2 - index1;
    }

    /**
     * list平均分成指定几份
     * @param source
     * @param n
     * @return
     */
    public static <T> List<List<T>> averageAssign(List<T> source, int n)
    {
        List<List<T>> result = new ArrayList<List<T>>();
        int remaider = source.size() % n; // (先计算出余数)
        int number = source.size() / n; // 然后是商
        int offset = 0;// 偏移量
        for (int i = 0; i < n; i++)
        {
            List<T> value = null;
            if (remaider > 0)
            {
                value =
                        source.subList(i * number + offset, (i + 1) * number
                                + offset + 1);
                remaider--;
                offset++;
            }
            else
            {
                value =
                        source.subList(i * number + offset, (i + 1) * number
                                + offset);
            }
            result.add(value);
        }
        return result;
    }

    /**
     * 获取在任务中和挂起队列里的所有任务信息
     * @param nodeMap
     * @param pendingTasks
     * @return
     */
    private List<TaskInfo> getTaskInfoList(Map<Integer,List<TaskInfo>> nodeMap,Map<Integer,Integer> pendingTasks)
    {
        List<TaskInfo> taskInfos= new ArrayList<TaskInfo>();
        if(nodeMap.size()>0 && !nodeMap.isEmpty())
        {
          for(Integer nodeId:nodeMap.keySet())
          {
              Integer nodeID=nodeId;
              if(null!=nodeMap.get(nodeID))
              {
                  taskInfos.addAll(nodeMap.get(nodeID));
              }

          }
        }
        if(pendingTasks.size()>0 && !pendingTasks.isEmpty())
        {
            TaskInfo taskInfo = new TaskInfo();
            for(Integer taskId:pendingTasks.keySet())
            {
                Integer taskID=taskId;
                taskInfo.setTaskId(taskID);
                taskInfo.setConsumption(pendingTasks.get(taskID));
                taskInfos.add(taskInfo);
            }
        }
      return taskInfos;
    }
}
