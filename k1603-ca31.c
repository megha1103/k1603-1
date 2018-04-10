#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
#include"helper.h"
#include"banker.h"

int Banker_init(struct BankerData *data, int availableResourcesCount,int processCount, 
        int* maxResourcesArray, int** resourcesDemandMatrix, int** resourcesAllocatedMatrix)
   // Takes  Banker Data structure and corresponding data to initialize it.
{
    if(availableResourcesCount<1)return -1;
    
    // Initialise the concurrencyLock of our Banker Data structure.
    pthread_mutex_init(&(data->concurrencyLock), NULL);
    // Store the data in our Banker Data structure.
    data->processCount = processCount;
    data->availableResourcesCount = availableResourcesCount;
    data->maxResourcesArray = maxResourcesArray;
    data->availableResourcesArray = (int*)malloc((availableResourcesCount)*sizeof(int));
    int r,sum,p;
    for(r=0;r<availableResourcesCount;++r)
    {
        sum = 0;
        for(p=0;p<processCount;++p)
        {
            sum += resourcesAllocatedMatrix[p][r];
        }
        data->availableResourcesArray[r] = maxResourcesArray[r] - sum;
    }
    data->resourcesDemandMatrix = resourcesDemandMatrix;
    data->resourcesAllocatedMatrix = resourcesAllocatedMatrix;
    // Compute resourcesRequiredMatrix
    data->resourcesRequiredMatrix = (int**)malloc(processCount * sizeof(int));
    for(p=0;p<processCount;++p)
    {
        data->resourcesRequiredMatrix[p] = (int*)malloc(availableResourcesCount*sizeof(int));
    }
    for(p=0;p<processCount;++p)
    {
        for(r=0;r<availableResourcesCount;++r)
        {
            data->resourcesRequiredMatrix[p][r] = data->resourcesDemandMatrix[p][r] - data->resourcesAllocatedMatrix[p][r];
        }
    }
    return 1;
}
void Banker_destroy(struct BankerData* banker)
{
    free(banker->availableResourcesArray);
    free(banker->resourcesRequiredMatrix);
}


int Banker_freeResource(struct BankerData *banker,int processIndex, int resourceIndex, int resourceCount)
{
   
    pthread_mutex_lock(&(banker->concurrencyLock));
    if(resourceIndex<0 || resourceIndex>(banker->availableResourcesCount)-1
        || processIndex<0 || processIndex>(banker->processCount)-1) 
        {
            // Unlock the mutex lock to allow other threads to allocate resources.
            pthread_mutex_unlock(&(banker->concurrencyLock));
            return -1;
        }
    if(resourceCount > banker->resourcesAllocatedMatrix[processIndex][resourceIndex])
        resourceCount = banker->resourcesAllocatedMatrix[processIndex][resourceIndex];

    // Free the resource and return it to the banker.
    banker->resourcesAllocatedMatrix[processIndex][resourceIndex] -= resourceCount;
    banker->availableResourcesArray[resourceIndex] += resourceCount;
    banker->resourcesRequiredMatrix[processIndex][resourceIndex] += resourceCount;

    // Unlock the mutex lock to allow other threads to access the banker's data.
    pthread_mutex_unlock(&(banker->concurrencyLock));
    return 1;
}

int Banker_freeAllResources(struct BankerData *banker,int processIndex)
{
    if(processIndex<0 || processIndex>(banker->processCount)-1) return -1;
    int i;
    // Use a mutex lock to prevent concurrent access to the banker's data.
    pthread_mutex_lock(&(banker->concurrencyLock));
    for(i=0;i<banker->availableResourcesCount;++i)
    {
        banker->availableResourcesArray[i] += banker->resourcesAllocatedMatrix[processIndex][i];
        banker->resourcesRequiredMatrix[processIndex][i] += banker->resourcesAllocatedMatrix[processIndex][i];
        banker->resourcesAllocatedMatrix[processIndex][i] = 0;
    }
    // Unlock the mutex lock to allow other threads to access the banker's data.
    pthread_mutex_unlock(&(banker->concurrencyLock));
    return 1;
}

/*
    Gives resource to the caller if it is available and does not lead to an unsafe state.
    This is a thread safe function. It uses mutex locks to acomplish thread safety.
    Parameters: banker,         the BankerData structure storing the current state of the banker.
                resourceIndex,  the index number of the resource to allocate to the caller.
                resourceCount,  the number of resource instances to allocate to the caller.
    Return:  1 on successfull resource allocation.
            -1 if the resourceIndex is invalid.
            -2 if processIndex is invalid.
            -3 if process is requests more resources than its max requirement or resourceCount is less than 0.
            -4 if resource allocation cannot be done due to unsafe state.
            -5 if resources requested are currently unavailable.
*/
int Banker_requestResource(struct BankerData *banker,int processIndex, int resourceIndex, int resourceCount)
{
    // Check for validity of passed parameters.
    if(processIndex<0 || processIndex>(banker->processCount)-1)return -2;
    // This will be returned the the function.
    int returnCode = 1;

    // Use a mutex lock to prevent concurrent resource allocation.
    pthread_mutex_lock(&(banker->concurrencyLock));

    // Check for validity of passed parameters.
    if(resourceIndex<0 || resourceIndex>(banker->availableResourcesCount)-1)returnCode = -1;
    if(resourceCount+banker->resourcesAllocatedMatrix[processIndex][resourceIndex]>(banker->resourcesDemandMatrix[processIndex][resourceIndex]) || resourceCount<0)returnCode = -3;
    // Check if requested resources are available.
    if(resourceCount>banker->availableResourcesArray[resourceIndex])returnCode = -5;
    if(returnCode<1)
    {
        // Unlock the mutex lock to allow other threads to allocate resources.
        pthread_mutex_unlock(&(banker->concurrencyLock));
        return returnCode;
    }

    // Allocate the resources to simulate if the banker will be in a safe state.
    banker->resourcesAllocatedMatrix[processIndex][resourceIndex]+=resourceCount;
    banker->availableResourcesArray[resourceIndex]-=resourceCount;
    banker->resourcesRequiredMatrix[processIndex][resourceIndex]-=resourceCount;

    int* safeSequence = Banker_getSafeSequence(banker);
    // If the new state is unsafe.. , revert changes and set returnCode to signal faliure in allocation.
    if(safeSequence == NULL)
    {
        // Revert allocated resources.
        banker->resourcesAllocatedMatrix[processIndex][resourceIndex]-=resourceCount;
        banker->availableResourcesArray[resourceIndex]+=resourceCount;
        banker->resourcesRequiredMatrix[processIndex][resourceIndex]+=resourceCount;
        returnCode = -4;
    }
    else
    {
        // Free the safeSequence array after use.
        free(safeSequence);
    }

    // Unlock the mutex lock to allow other threads to allocate resources.
    pthread_mutex_unlock(&(banker->concurrencyLock));

    return returnCode;
}

/*
    Checks if the passed BankerData is in a safe state and returns the safe sequence.
    Return:  NULL, if the BankerData is not in a safe state i.e. there is no safe sequence.
    int*, array of size processCount(of the BankerData passed) which stores the index of processes, in the safe sequence.
*/
int* Banker_getSafeSequence(struct BankerData *banker)
{
    // Allocate memory to store the safe sequence.
    int* safeSequence = calloc(banker->processCount,sizeof(int));
    int safeSequenceMarker = 0;

    // This stores the no. of processes remaining for simulated execution.
    int remainingProcesses = banker->processCount;

    // This set to 0 if there is a deadlock during the simulation.
    int isStatePseudoSafe;
    int p, r;

    // This stores the execution state of each process while simulation.
    int *hasFinished = calloc(banker->processCount,sizeof(int));

    // This is a temporary array used in the simulation.
    int *availableResourcesArray = malloc((banker->availableResourcesCount)*sizeof(int));
    // Copy banker data into temporary array
    for(r=0;r<banker->availableResourcesCount;++r)
        availableResourcesArray[r]=banker->availableResourcesArray[r];
    

    // Simulate resource allocation to find a safe sequence.
    while(remainingProcesses>0)
    {
        isStatePseudoSafe = 0;
        for(p=0;p<banker->processCount;++p)
        {
            // Simulate this process if it has not yet finished.
            if(hasFinished[p]!=1)
            {
                // Check if this process can allocate all resources.
                for(r=0;r<banker->availableResourcesCount;++r)
                {
                    if(banker->resourcesRequiredMatrix[p][r]>availableResourcesArray[r])
                    {
                        // This process cannot allocate all resources.
                        break;
                    }
                }
                if(r==banker->availableResourcesCount)
                {
                    // This process can allocate all resources.
                    // After process finishes, it returns its resources to the banker
                    for(r=0;r<banker->availableResourcesCount;++r)
                    {
                        availableResourcesArray[r] += banker->resourcesAllocatedMatrix[p][r];
                    }

                    // Decrements remainingProcesses
                    --remainingProcesses;
                    // Sets its execution state
                    hasFinished[p]=1;
                    // Appends itself to the safe sequence.
                    safeSequence[safeSequenceMarker] = p;
                    ++safeSequenceMarker;

                    // Sets the current banker's state to pseudo safe
                    isStatePseudoSafe = 1;
                }
            }
        }
        // No process executed in this iteration, the system resulted in a deadlock.
        if(isStatePseudoSafe != 1)break;
    }

    
    free(hasFinished);
    free(availableResourcesArray);
    if(remainingProcesses>0)
    {
        
        free(safeSequence);
        return NULL;
    }
    return safeSequence;
}

/*
    Used for debugging.
    Prints the data of the given banker structure.
*/
void Banker_displayBanker(struct BankerData *data)
{
    int m,j,k;
    printf("\tBanker Data\n");
    int** matrices[] = {data->resourcesDemandMatrix,data->resourcesAllocatedMatrix,data->resourcesRequiredMatrix};
    printf("\t Available Resources { ");
    for(k=0;k<data->availableResourcesCount;++k)
        printf("%d ",data->availableResourcesArray[k]);
    printf("}\n");
    for(k=0;k<arraylength(matrices);++k)
    {
        printf("\t  %s Matrix  :-\n",
        (k==0)?"Recource demand":(k==1)?" Allocated":" Required");
        for(m=0;m<data->processCount;++m)
        {
            printf("\t\t");
            for(j=0;j<data->availableResourcesCount;++j)
            {
                printf("%d ",matrices[k][m][j]);
            }
            printf("\n");
        }
    }
}
