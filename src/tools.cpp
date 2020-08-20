#include "def.h"

bool QualifyTextual(Record *rx, Record *ry, int xstart, int ystart, int o)
{
  int left_th = rx->length + ry->length;
  int xcounter = xstart, ycounter = ystart, overlap = o;
  int q_intersect = ry->requiredOverlap;

  while ((xcounter < rx->length) && (ycounter < ry->length))
  {
    if (FLAGS_inline_records == 1)
    {
      if (rx->tids[xcounter] == ry->tids[ycounter])
      {
        overlap++;
        if (overlap >= q_intersect)
          return true;

        if (left_th-- < 0)
          return false;

        xcounter++;
        ycounter++;
      }
      else if (rx->tids[xcounter] > ry->tids[ycounter])
      {
        if (left_th-- < 0)
          return false;
        ycounter++;
      }
      else
      {
        if (left_th-- < 0)
          return false;
        xcounter++;
      }
    }
    else
    {
      if (rx->keywords[xcounter] == ry->keywords[ycounter])
      {
        overlap++;
        if (overlap >= q_intersect)
          return true;

        if (left_th-- < 0)
          return false;

        xcounter++;
        ycounter++;
      }
      else if (rx->keywords[xcounter] > ry->keywords[ycounter])
      {
        if (left_th-- < 0)
          return false;
        ycounter++;
      }
      else
      {
        if (left_th-- < 0)
          return false;
        xcounter++;
      }
    }
  }

  return (overlap >= q_intersect);
}

bool QualifyPositionalFilter(Record *rx, int xpos, Record *ry, int ypos)
{
  ry->requiredOverlap =
      int((rx->length + ry->length) * FLAGS_threshold / (1.0 + FLAGS_threshold) - EPS) + 1;

  return ((rx->length - xpos >= ry->requiredOverlap) &&
          (ry->length - ypos >= ry->requiredOverlap));
}

// Same as VerifyAllPairs but generates results locally
void VerifyAllPairsThread(Record *rx, Record *ry, int overlap,
                          uint64_t &n_candidates, uint64_t &n_results)
{
  n_candidates = 0;
  n_results = 0;

  ry->requiredOverlap =
      int((rx->length + ry->length) * FLAGS_threshold / (1.0 + FLAGS_threshold) - EPS) + 1;

  if (FLAGS_inline_records == 1)
  {
    if (rx->tids[rx->probePrefixLength - 1] < ry->tids[ry->indexPrefixLength - 1])
    {
      if (overlap + rx->length - rx->probePrefixLength >= ry->requiredOverlap)
      {
        ++n_candidates;

        if (QualifyTextual(rx, ry, rx->probePrefixLength, overlap, overlap))
        {
          ++n_results;

        }
      }
    }
    else
    {
      if (overlap + ry->length - ry->indexPrefixLength >= ry->requiredOverlap)
      {
        ++n_candidates;

        if (QualifyTextual(rx, ry, overlap, ry->indexPrefixLength, overlap))
        {
          ++n_results;
        }
      }
    }
  }
  else
  {
    if (rx->keywords[rx->probePrefixLength - 1] <
        ry->keywords[ry->indexPrefixLength - 1])
    {
      if (overlap + rx->length - rx->probePrefixLength >= ry->requiredOverlap)
      {
        ++n_candidates;

        if (QualifyTextual(rx, ry, rx->probePrefixLength, overlap, overlap))
        {
          ++n_results;
        }
      }
    }
    else
    {
      if (overlap + ry->length - ry->indexPrefixLength >= ry->requiredOverlap)
      {
        ++n_candidates;

        if (QualifyTextual(rx, ry, overlap, ry->indexPrefixLength, overlap))
        {
          ++n_results;
        }
      }
    }
  }
}

void VerifyAllPairs(Record *rx, Record *ry, int overlap)
{
  ry->requiredOverlap =
      int((rx->length + ry->length) * FLAGS_threshold / (1.0 + FLAGS_threshold) - EPS) + 1;

  if (FLAGS_inline_records == 1)
  {
    if (rx->tids[rx->probePrefixLength - 1] <
        ry->tids[ry->indexPrefixLength - 1])
    {
      if (overlap + rx->length - rx->probePrefixLength >= ry->requiredOverlap)
      {
        numCandidates++;

        if (QualifyTextual(rx, ry, rx->probePrefixLength, overlap, overlap))
        {
          numResults++;

        }
      }
    }
    else
    {
      if (overlap + ry->length - ry->indexPrefixLength >= ry->requiredOverlap)
      {
        numCandidates++;

        if (QualifyTextual(rx, ry, overlap, ry->indexPrefixLength, overlap))
        {
          numResults++;
        }
      }
    }
  }
  else
  {
    if (rx->keywords[rx->probePrefixLength - 1] <
        ry->keywords[ry->indexPrefixLength - 1])
    {
      if (overlap + rx->length - rx->probePrefixLength >= ry->requiredOverlap)
      {
        numCandidates++;

        if (QualifyTextual(rx, ry, rx->probePrefixLength, overlap, overlap))
        {
          numResults++;
        }
      }
    }
    else
    {
      if (overlap + ry->length - ry->indexPrefixLength >= ry->requiredOverlap)
      {
        numCandidates++;

        if (QualifyTextual(rx, ry, overlap, ry->indexPrefixLength, overlap))
        {
          numResults++;
        }
      }
    }
  }
}

